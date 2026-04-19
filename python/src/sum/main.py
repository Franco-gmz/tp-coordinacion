import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import InternalMsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

DATA_MESSAGE_FIELD_COUNT = 3
EOF_MESSAGE_FIELD_COUNT = 1

class SumFilter:
    def __init__(self):

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"])

        self._init_control_channel()
        self._init_output_channel()
        self._init_local_state()

        self.closed = False
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    """ 
    Initialize in-memory state used to accumulate fruit amounts by client
    and synchronize access between the data and control consumer threads.
    """
    def _init_local_state(self):
        self.amount_by_client = {}
        self.lock = threading.Lock()

    """ 
    Initialize all control exchange outputs used to broadcast coordination
    messages to every sum worker instance.
    """
    def _init_control_channel(self):

        self.control_outputs = []

        for i in range(SUM_AMOUNT):
            control_output = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{i}"])
            self.control_outputs.append(control_output)

    """ 
    Initialize all aggregation outputs used to send aggregated fruit data
    to the corresponding aggregation worker based on routing logic.
    """
    def _init_output_channel(self):

        self.data_outputs = []

        for i in range(AGGREGATION_AMOUNT):
            data_output = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"])
            self.data_outputs.append(data_output)

    """
    Gracefully handle SIGTERM shutdown requests.

    The worker marks itself as closed, stops accepting new messages,
    closes all RabbitMQ inputs and outputs, notifies aggregation workers
    that this sum worker is disconnecting, and then releases remaining
    output connections.
    """
    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")

        self.closed = True

        try:
            self.input_queue.close()
        except Exception:
            pass

        try:
            self.control_input.close()
        except Exception:
            pass

        for control_output in self.control_outputs:
            try:
                control_output.close()
            except Exception:
                pass

        self._notify_disconnect()

        for data_output in self.data_outputs:
            try:
                data_output.close()
            except Exception:
                pass

    """
    Notify all aggregation workers that this sum worker became unavailable.

    This allows downstream workers to stop waiting for EOF messages from
    this worker and continue closing pending clients if necessary.
    """
    def _notify_disconnect(self):
        for output in self.data_outputs:
            output.send(message_protocol.internal.serialize([InternalMsgType.SUM_WORKER_SHUTDOWN.value, ID]))

    """
    Store and accumulate fruit amounts for a given client.

    Access is protected with a lock because this state can also be read
    and deleted by the control consumer thread while processing EOF
    coordination messages.
    """
    def _process_data(self, client_id, fruit, amount):
        with self.lock:

            if client_id not in self.amount_by_client:
                self.amount_by_client[client_id] = {}

            current_amount = self.amount_by_client[client_id].get(fruit, fruit_item.FruitItem(fruit, 0))
            self.amount_by_client[client_id][fruit] = current_amount + fruit_item.FruitItem(fruit, int(amount))

    """
    Process incoming data messages from the input queue.

    A message can contain either:
    - Fruit data for a client
    - An EOF notification indicating no more records will arrive for that client

    Messages are acknowledged only after being processed successfully.
    Invalid or unexpected messages are rejected with nack().
    """
    def process_data_messsage(self, message, ack, nack):
        
        try:
            if self.closed:
                logging.info("Ignoring data message because worker is shutting down")
                nack()
                return
            
            fields = message_protocol.internal.deserialize(message)

            if len(fields) == DATA_MESSAGE_FIELD_COUNT:
                self._process_data(*fields)
            elif len(fields) == EOF_MESSAGE_FIELD_COUNT:
                client_id = fields[0]
                self._broadcast(InternalMsgType.CLIENT_EOF, client_id)
            else:
                logging.exception(f"Unknown data message")
                nack()
                return
            ack()
        except Exception as e:
            logging.exception(f"Error processing data message: {e}")
            nack()

    """
    Start both consumers used by the sum worker.

    A background thread is created for the control exchange consumer so it can
    receive coordination messages in parallel while the main thread processes
    input data messages from the queue.
    """
    def start(self):
        control_thread = threading.Thread(target=lambda: self.control_input.start_consuming(self.process_control_message),daemon=True)
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

    """
    Broadcast a control message to all sum workers.

    Control messages are used to coordinate client completion between sums,
    allowing every worker to know when a client has finished sending records.
    """
    def _broadcast(self, msgtyp: InternalMsgType, client_id):

        logging.info(f"Broadcasting control '{msgtyp.value}' for client {client_id} from sum_{ID}")
        for control_output in self.control_outputs:
            control_output.send(message_protocol.internal.serialize([msgtyp.value, ID, client_id]))  

    """
    Notify all aggregation workers that this sum worker has finished sending
    data for the given client.

    After broadcasting EOF messages, the local in-memory state associated
    with the client is removed.
    """
    def _close_client(self, client_id):

        logging.info(f"Sending EOF for client {client_id}")

        msgtyp = InternalMsgType.CLIENT_EOF.value
        for data_output in self.data_outputs:
            data_output.send(message_protocol.internal.serialize([msgtyp, client_id, ID]))

        if client_id in self.amount_by_client:
            del self.amount_by_client[client_id]

    """
    Send all locally aggregated fruit totals for a client to their
    corresponding aggregation workers.

    Each fruit is routed to a specific aggregation worker using a stable
    hash-based partitioning strategy.
    """
    def _dispatch_aggregated_data(self, client_id):

        with self.lock:
            logging.info(f"Sending aggregated data for client {client_id}")

            if client_id in self.amount_by_client:

                client_fruits = self.amount_by_client.get(client_id, {})

                for final_fruit_item in client_fruits.values():
                    agg_id = self._get_agg_id(client_id, final_fruit_item.fruit)
                    output = self.data_outputs[agg_id]
                    msgtyp = InternalMsgType.PROCESS_DATA.value
                    output.send(message_protocol.internal.serialize([msgtyp, client_id, final_fruit_item.fruit, final_fruit_item.amount]))

    """
    Process control messages received from the sum control exchange.

    When a CLIENT_EOF message is received, the worker dispatches all locally
    aggregated data for the client to the aggregation workers and then sends
    EOF notifications indicating no more data will be produced for that client.

    Valid messages are acknowledged after successful processing. Invalid or
    unexpected messages are rejected with nack().
    """
    def process_control_message(self, message, ack, nack):

        try:

            if self.closed:
                logging.info("Ignoring control message because worker is shutting down")
                nack()
                return

            fields = message_protocol.internal.deserialize(message)

            msgtyp = InternalMsgType(fields[0])
            client_id = fields[2]
            
            if msgtyp == InternalMsgType.CLIENT_EOF:
                self._dispatch_aggregated_data(client_id)
                self._close_client(client_id)
                ack()
            else:
                nack()

        except Exception as e:
            logging.exception(f"Error processing control message: {e}")
            nack()

    def _get_agg_id(self, client_id, fruit):
        key = f"{client_id}:{fruit}".encode("utf-8")
        digest = hashlib.md5(key).hexdigest()
        return int(digest, 16) % AGGREGATION_AMOUNT

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
