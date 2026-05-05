from enum import Enum


class SilverConnectionState(Enum):
    NEW = "new"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CLOSED_DOWN = "closed_down"
