import json
from enum import Enum

class InternalMsgType(Enum):
    CLIENT_EOF = "client_eof"
    AGGREGATOR_READY = "aggregator_ready"
    SUM_WORKER_SHUTDOWN = "sum_worker_shutdown"
    AGG_WORKER_SHUTDOWN = "agg_worker_shutdown"
    PROCESS_DATA = "process_data"

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
