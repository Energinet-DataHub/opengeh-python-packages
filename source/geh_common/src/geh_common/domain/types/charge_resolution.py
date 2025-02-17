from enum import Enum


class ChargeResolution(Enum):
    MONTH = "P1M"
    """Applies to subscriptions and fees"""
    DAY = "P1D"
    """Applies to tariffs"""
    HOUR = "PT1H"
    """Applies to tariffs"""
