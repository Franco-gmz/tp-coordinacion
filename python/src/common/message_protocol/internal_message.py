from enum import Enum

class InternalMsgType(Enum):
    ALL_DATA_SENT_TO_AGGREGATOR = "all_data_sent_to_aggregator"
    CLIENT_EOF = "client_eof"
