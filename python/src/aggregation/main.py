import os
import logging
import bisect
import threading
import hashlib

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import InternalMsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

AGG_CONTROL_EXCHANGE = "AGG_CONTROL_EXCHANGE"

SUM_DATA_MESSAGE_FIELD_COUNT = 4
SUM_EOF_MESSAGE_FIELD_COUNT = 3
SUM_SHUTDOWN_MESSAGE_FIELD_COUNT = 2

class AggregationFilter:

    def __init__(self):

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGG_CONTROL_EXCHANGE, [f"{AGGREGATION_PREFIX}_{ID}"])

        self._init_local_state()

        
    def _init_local_state(self):

        self.fruit_top_by_client = {}
        self.eof_received = {}
        self.sum_workers = SUM_AMOUNT

    """
    Store and accumulate fruit amounts for a given client.

    Fruit entries are kept in a sorted list so the top results can be built
    efficiently later. If the fruit already exists for the client, its amount
    is updated. Otherwise, a new FruitItem is inserted while preserving order.
    """
    def _process_data(self, client_id, fruit, amount):

        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []

        top_fruit = self.fruit_top_by_client[client_id]

        for i in range(len(top_fruit)):
            if top_fruit[i].fruit == fruit:
                top_fruit[i] = top_fruit[i] + fruit_item.FruitItem(fruit, int(amount))
                return

        bisect.insort(top_fruit, fruit_item.FruitItem(fruit, int(amount)))

    """
    Track EOF notifications received from sum workers for a given client.

    Once EOF has been received from all active sum workers, the aggregation
    worker sends its partial aggregated data downstream and notifies the join
    stage that no more data will be produced for that client.
    """
    def _process_eof(self, client_id, sum_id):
        logging.info(f"Received EOF from {sum_id} for client {client_id}")

        if client_id not in self.eof_received:
            self.eof_received[client_id] = set()

        self.eof_received[client_id].add(sum_id)
        
        # Use >= because a disconnected sum worker may have already sent its EOF
        # before shutting down, leaving more EOFs than active workers.
        if len(self.eof_received[client_id]) >= self.sum_workers:
            self._send_aggregated_data(client_id)
            self._close_client(client_id)

    """
    Send the aggregated fruit totals currently stored for a client.

    The generated top is partial because each aggregation worker only contains
    a subset of the client's fruits. The join stage is responsible for merging
    the partial tops from all aggregation workers into the final result.
    """
    def _send_aggregated_data(self, client_id):

        fruits = self.fruit_top_by_client.get(client_id, [])
        fruit_top = list(map(lambda current_fruit_item: (current_fruit_item.fruit,current_fruit_item.amount),fruits))
        logging.info(f"Sending partial top of client {client_id} from agg_{ID}")

        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))

    """
    Notify the join stage that this aggregation worker finished processing
    the given client.

    After sending the final EOF message, the local aggregation state for
    the client is removed from memory.
    """
    def _close_client(self, client_id):

        logging.info(f"Sending final EOF for client {client_id}")
        self.output_queue.send(message_protocol.internal.serialize([InternalMsgType.CLIENT_EOF.value, client_id, ID]))
        if client_id in self.fruit_top_by_client:
            del self.fruit_top_by_client[client_id]

    """
    Process incoming messages received from sum workers.

    Supported message types include:
    - PROCESS_DATA: aggregated fruit amounts for a client
    - CLIENT_EOF: notification that a sum worker finished sending data for a client
    - SUM_WORKER_SHUTDOWN: notification that a sum worker became unavailable

    Each message type is validated against its expected field count before
    being processed. Valid messages are acknowledged after successful
    processing. Invalid or malformed messages are rejected with nack().
    """
    def process_message(self, message, ack, nack):

        try:
            logging.info("Process message")
            
            fields = message_protocol.internal.deserialize(message)
            msgtyp = InternalMsgType(fields[0])

            if msgtyp == InternalMsgType.PROCESS_DATA:

                if len(fields) != SUM_DATA_MESSAGE_FIELD_COUNT:
                    logging.error(f"Process data message expects {SUM_DATA_MESSAGE_FIELD_COUNT} fields but received {len(fields)}")
                    nack()
                    return
                self._process_data(*fields[1:])

            elif msgtyp == InternalMsgType.CLIENT_EOF:
                if len(fields) != SUM_EOF_MESSAGE_FIELD_COUNT:
                    logging.error(f"Process EOF message expects {SUM_EOF_MESSAGE_FIELD_COUNT} fields but received {len(fields)}")
                    nack()
                    return
                self._process_eof(*fields[1:])

            elif msgtyp == InternalMsgType.SUM_WORKER_SHUTDOWN:
                if len(fields) != SUM_SHUTDOWN_MESSAGE_FIELD_COUNT:
                    logging.error(f"Process EOF message expects {SUM_SHUTDOWN_MESSAGE_FIELD_COUNT} fields but received {len(fields)}")
                    nack()
                    return
                self._handle_sum_shutdown(*fields[1:])

            ack()
        except ValueError:
            logging.error(f"Unknown message type received: {fields[0]}")
            nack()
        except Exception as e:
            logging.exception(f"Error parsing message type: {e}")
            nack()

    """
    Handle the shutdown of a sum worker.

    Any data already received from the disconnected worker is still considered
    valid and remains part of the aggregation state. The shutdown only reduces
    the number of active sum workers expected for future EOF coordination.
    """
    def _handle_sum_shutdown(self, sum_id):
        logging.info(f"Shutdown received from sum_{sum_id}")
        self.sum_workers -=1

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0

if __name__ == "__main__":
    main()
