import os
import logging
import bisect
import threading
import hashlib

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal_message import InternalMsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

AGG_CONTROL_EXCHANGE = "AGG_CONTROL_EXCHANGE"


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGG_CONTROL_EXCHANGE, [f"{AGGREGATION_PREFIX}_{ID}"]
        )

        self.control_outputs = []
        for i in range(AGGREGATION_AMOUNT):
            control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGG_CONTROL_EXCHANGE, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.control_outputs.append(control_output)

        self.fruit_top_by_client = {}
        self.clients_ready = {}
        self.partial_top_sent = set()
        self.final_eof_sent = set()
        self.closed_clients = set()

        self.eof_received = {}

        self.lock = threading.RLock()

    def _get_client_owner(self, client_id):
        return int(hashlib.md5(client_id.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT

    def _broadcast(self, msgtyp: InternalMsgType, client_id):
        logging.info(
            f"Broadcasting control '{msgtyp.value}' for client {client_id} from aggregation_{ID}"
        )

        for control_output in self.control_outputs:
            control_output.send(
                message_protocol.internal.serialize([msgtyp.value, ID, client_id])
            )

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            if client_id in self.closed_clients:
                return

            #logging.info("Processing data message")

            if client_id not in self.fruit_top_by_client:
                self.fruit_top_by_client[client_id] = []

            top_fruit = self.fruit_top_by_client[client_id]

            for i in range(len(top_fruit)):
                if top_fruit[i].fruit == fruit:
                    top_fruit[i] = top_fruit[i] + fruit_item.FruitItem(fruit, int(amount))
                    return

            bisect.insort(top_fruit, fruit_item.FruitItem(fruit, int(amount)))

    def _process_sum_eof(self, client_id, sum_id):
        logging.info(f"Received EOF from {sum_id} for client {client_id}")

        if client_id not in self.eof_received:
            self.eof_received[client_id] = set()

        self.eof_received[client_id].add(sum_id)
        
        if len(self.eof_received[client_id]) == SUM_AMOUNT:
            self._send_top(client_id)
            self._send_eof_to_join(client_id)

    def _send_top(self, client_id):
        with self.lock:
            if client_id in self.partial_top_sent:
                return

            self.partial_top_sent.add(client_id)

            fruits = self.fruit_top_by_client.get(client_id, [])

            #fruit_chunk = list(fruits[-TOP_SIZE:])
            #fruit_chunk.reverse()

            fruit_top = list(
                map(
                    lambda current_fruit_item: (
                        current_fruit_item.fruit,
                        current_fruit_item.amount,
                    ),
                    fruits,
                )
            )

            logging.info(f"Sending partial top for client {client_id, fruit_top}")

            self.output_queue.send(
                message_protocol.internal.serialize([client_id, fruit_top])
            )

    def _send_eof_to_join(self, client_id):
        with self.lock:
            if client_id in self.final_eof_sent:
                return

            self.final_eof_sent.add(client_id)
            self.closed_clients.add(client_id)

            logging.info(f"Sending final EOF for client {client_id}")

            self.output_queue.send(
                message_protocol.internal.serialize([InternalMsgType.EOF.value, client_id, ID])
            )

            if client_id in self.fruit_top_by_client:
                del self.fruit_top_by_client[client_id]

            if client_id in self.clients_ready:
                del self.clients_ready[client_id]

    def process_message(self, message, ack, nack):
        #logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            client_id = fields[0]
            sum_id = fields[1]
            self._process_sum_eof(client_id, sum_id)

        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msgtyp = InternalMsgType(fields[0])

        if msgtyp == InternalMsgType.AGGREGATOR_READY:
            agg_id = fields[1]
            client_id = fields[2]

            with self.lock:
                if client_id in self.closed_clients:
                    ack()
                    return

                if client_id not in self.clients_ready:
                    self.clients_ready[client_id] = set()

                self.clients_ready[client_id].add(agg_id)
                current_count = len(self.clients_ready[client_id])

                logging.info(
                    f"Received control '{msgtyp.value}' from aggregation_{agg_id} "
                    f"for client {client_id}. Count: {current_count}/{AGGREGATION_AMOUNT}"
                )

                if current_count == AGGREGATION_AMOUNT:
                    self._send_top(client_id)

                    owner = self._get_client_owner(client_id)
                    if owner == ID:
                        self._send_eof_to_join(client_id)

        ack()

    def _get_client_owner(self, client_id):
        return int(hashlib.md5(client_id.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT

    def start(self):
        control_thread = threading.Thread(
            target=lambda: self.control_input.start_consuming(
                self.process_control_message
            ),
            daemon=True
        )
        control_thread.start()

        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()