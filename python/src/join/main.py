import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import InternalMsgType

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

AGG_DATA_PARAMS = 3
AGG_EOF_PARAMS = 3
AGG_SHUTDOWN_PARAMS = 2

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

        self.amount_by_client = {}
        self.eof_received = {}
        self.agg_workers = AGGREGATION_AMOUNT

    """
    Merge a partial top received from an aggregation worker into the local
    accumulated totals for the given client.

    Fruit amounts are stored in a dictionary so repeated fruits coming from
    different aggregation workers can be combined efficiently.
    """
    def _process_partial_top(self, client_id, fruit_top):

        if client_id not in self.amount_by_client:
            self.amount_by_client[client_id] = {}

        current_amounts = self.amount_by_client[client_id]
        for fruit, amount in fruit_top:
            current_amounts[fruit] = current_amounts.get(fruit, 0) + int(amount)

    """
    Processes an EOF notification from an aggregation worker for a specific client.

    Tracks which aggregation workers have already sent EOF for the client.
    Once EOF has been received from all aggregation workers, computes the final
    top fruits ranking, sends the result to the output queue, and removes the
    client's stored aggregation data to free memory.
    """
    def _process_eof(self, client_id, agg_id):

        if client_id not in self.eof_received:
            self.eof_received[client_id] = set()

        self.eof_received[client_id].add(agg_id)

        if len(self.eof_received[client_id]) >= self.agg_workers:

            fruits = self.amount_by_client.get(client_id, {})
            final_top = sorted(fruits.items(),key=lambda item: item[1],reverse=True)[:TOP_SIZE]

            self.output_queue.send(message_protocol.internal.serialize([client_id, final_top]))

            if client_id in self.amount_by_client:
                del self.amount_by_client[client_id]

    """
    Process incoming messages received from aggregation workers.

    Supported message types include:
    - PROCESS_DATA: partial aggregated fruit totals for a client
    - CLIENT_EOF: notification that an aggregation worker finished sending
      data for a client
    - AGG_WORKER_SHUTDOWN: notification that an agg worker became unavailable

    Each message type is validated against its expected field count before
    being processed. Valid messages are acknowledged after successful
    processing. Invalid or malformed messages are rejected with nack().
    """
    def process_messsage(self, message, ack, nack):

        try:

            fields = message_protocol.internal.deserialize(message)
            msgtyp = InternalMsgType(fields[0])

            if msgtyp == InternalMsgType.PROCESS_DATA:

                if len(fields) != AGG_DATA_PARAMS:
                    logging.error(f"Process data message expects {AGG_DATA_PARAMS} fields but received {len(fields)}")
                    nack()
                    return

                self._process_partial_top(*fields[1:])

            elif msgtyp == InternalMsgType.CLIENT_EOF:
                
                if len(fields) != AGG_EOF_PARAMS:
                    logging.error(f"Process EOF message expects {AGG_EOF_PARAMS} fields but received {len(fields)}")
                    nack()
                    return
                
                self._process_eof(*fields[1:])
            
            elif msgtyp == InternalMsgType.AGG_WORKER_SHUTDOWN:

                if len(fields) != AGG_SHUTDOWN_PARAMS:
                    logging.error(f"Process agg shutdown message expects {AGG_SHUTDOWN_PARAMS} fields but received {len(fields)}")
                    nack()
                    return
                
                self._handle_agg_shutdown(*fields[1:])

            ack()
        except ValueError:
            logging.error(f"Unknown message type received: {fields[0]}")
            nack()
        except Exception as e:
            logging.exception(f"Error parsing message type: {e}")
            nack()
    
    """
    Handles the shutdown notification of an aggregation worker.
    Decreases the number of active aggregation workers remaining in the system.
    """
    def _handle_agg_shutdown(self, agg_id):
        logging.info(f"Shutdown received from sum_{agg_id}")
        self.agg_workers -=1

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()
