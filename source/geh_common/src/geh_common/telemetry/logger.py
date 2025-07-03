import logging
from typing import Any

import geh_common.telemetry.logging_configuration as config


class Logger:
    def __init__(self, name: str, extras: dict[str, Any] | None = None) -> None:
        # Configuring logging with basicConfig ensures that log statements are
        # displayed in task logs in Databricks.
        logging.basicConfig(level=config.DEFAULT_LOG_LEVEL, format=config.DEFAULT_LOG_FORMAT)
        self._logger = logging.getLogger(name)
        self._logger.setLevel(config.DEFAULT_LOG_LEVEL)
        self._extras = (extras or {}) | config.get_extras()
        # According to DataHub 3.0 guide book
        self._extras["CategoryName"] = "Energinet.DataHub." + name

    def debug(self, message: str, extras: dict[str, Any] | None = None) -> None:
        extras = (extras or {}) | self._extras
        self._logger.debug(message, extra=extras)

    def info(self, message: str, extras: dict[str, Any] | None = None) -> None:
        extras = (extras or {}) | self._extras
        self._logger.info(message, extra=extras)

    def warning(self, message: str, extras: dict[str, Any] | None = None) -> None:
        extras = (extras or {}) | self._extras
        self._logger.warning(message, extra=extras)

    def error(self, message: str, extras: dict[str, Any] | None = None) -> None:
        extras = (extras or {}) | self._extras
        self._logger.error(message, extra=extras)
