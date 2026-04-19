import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal_message import InternalMsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )

        self.control_outputs = []
        for i in range(SUM_AMOUNT):
            control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{i}"]
            )
            self.control_outputs.append(control_output)

        self.closed_clients = set()

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            if client_id in self.closed_clients:
                return

            if client_id not in self.amount_by_client:
                self.amount_by_client[client_id] = {}

            self.amount_by_client[client_id][fruit] = self.amount_by_client[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            client_id = fields[0]
            self._broadcast(message_protocol.internal_message.InternalMsgType.CLIENT_EOF, client_id)

        ack()

    def start(self):
        control_thread = threading.Thread(
            target=lambda: self.control_input.start_consuming(
                self.process_control_message
            ),
            daemon=True
        )
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

    def _broadcast(self, msgtyp: message_protocol.internal_message.InternalMsgType, client_id):
        logging.info(f"Broadcasting control '{msgtyp.value}' for client {client_id} from sum_{ID}")

        for control_output in self.control_outputs:
            control_output.send(message_protocol.internal.serialize([msgtyp.value, ID, client_id]))  

    def _send_eof_to_agg(self, client_id):

        logging.info(f"Sending EOF for client {client_id}")

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id, ID]))

        if client_id in self.amount_by_client:
            del self.amount_by_client[client_id]

    def _send_to_aggregate(self, client_id):

        with self.lock:
            logging.info(f"Sending aggregated data for client {client_id}")

            if client_id in self.amount_by_client:

                client_fruits = self.amount_by_client.get(client_id, {})

                for final_fruit_item in client_fruits.values():
                    agg_id = self._get_agg_id(client_id, final_fruit_item.fruit)

                    self.data_output_exchanges[agg_id].send(
                        message_protocol.internal.serialize(
                            [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                        )
                    )

    def process_control_message(self, message, ack, nack):

        fields = message_protocol.internal.deserialize(message)

        msgtyp = InternalMsgType(fields[0])
        client_id = fields[2]
        
        if msgtyp == InternalMsgType.CLIENT_EOF:

            self._send_to_aggregate(client_id)
            self._send_eof_to_agg(client_id)

        ack()

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
