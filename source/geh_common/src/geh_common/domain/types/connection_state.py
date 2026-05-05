from enum import Enum


class ConnectionState(Enum):
    NEW = "new"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CLOSED_DOWN = "closed_down"
