import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.amount_by_client = {}

        self.eof_received = {}

    def _process_partial_top(self, client_id, fruit_top):
        if client_id not in self.amount_by_client:
            self.amount_by_client[client_id] = {}

        current_amounts = self.amount_by_client[client_id]
        logging.info(f"Process partial top {current_amounts} for client {client_id}")
        for fruit, amount in fruit_top:
            current_amounts[fruit] = current_amounts.get(fruit, 0) + int(amount)

    def _process_eof(self, client_id, agg_id):

        if client_id not in self.eof_received:
            self.eof_received[client_id] = set()

        self.eof_received[client_id].add(agg_id)

        if len(self.eof_received[client_id]) == AGGREGATION_AMOUNT:

            fruits = self.amount_by_client.get(client_id, {})

            final_top = sorted(
                fruits.items(),
                key=lambda item: item[1],
                reverse=True
            )[:TOP_SIZE]

            logging.info(f"Sending final top {[client_id, final_top]} for client {client_id}")

            self.output_queue.send(
                message_protocol.internal.serialize([client_id, final_top])
            )

            if client_id in self.amount_by_client:
                del self.amount_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) == 2:
            client_id = fields[0]
            fruit_top = fields[1]

            logging.info(f"Received partial top {fruit_top} for client {client_id}")
            self._process_partial_top(client_id, fruit_top)

        elif len(fields) == 3:
            client_id = fields[1]
            agg_id = fields[2]

            logging.info(f"Received final EOF from agg_{agg_id} for client {client_id}")
            self._process_eof(client_id, agg_id)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()
