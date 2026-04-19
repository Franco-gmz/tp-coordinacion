from enum import Enum

class InternalMsgType(Enum):
    PARTIAL_AGGREGATE = "partial_aggregate"
    CLIENT_EOF = "client_eof"
    ALL_DATA_SENT_TO_AGGREGATOR = "all_data_sent_to_aggregator"
    AGGREGATOR_READY = "aggregator_ready"
    EOF = "EOF"
    PROCESS = "Process"