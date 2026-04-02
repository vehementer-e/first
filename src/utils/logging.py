""" Logger helping to capture logs at the level of the console and also on Oasis (Optional)"""

import logging
from typing import Optional
import traceback
import http.client as http_client

from dmp_common.monitoring import FluentLogger


class DualLogger: # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """
    A logger that writes messages both to Python's standard logging system
    and optionally to Oasis via FluentLogger.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self,
                 name: str,
                 # level: str = ENV_VAR_LOG_LEVEL,
                 handler: Optional[logging.Handler] = None
                 ) -> None:

        # Standard Python logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Add a console handler if not already present
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        elif handler is not None:
            self.logger.addHandler(handler)

        # Custom fluent logger (optional, e.g. Fluentd)
        self.fluent_logger: Optional[FluentLogger] = FluentLogger(
                                                        ssl_context_args={
                                                            'cafile': '/certs/swift-ca.crt'},
                                                                  log_level=logging.INFO
                                                    )

    def _log(self, level: str, message: str, **kwargs):
        """Logs message at specified level to both loggers."""
        getattr(self.logger, level)(message)
        if self.fluent_logger:
            try:
                getattr(self.fluent_logger, level)(message, **kwargs)
            except Exception as e:
                self.logger.error(f" FluentLogger error: {e} \n "
                                  f"traceback.format_exc = {traceback.format_exc()}")

    def debug(self, message: str, **kwargs):
        """Logs a debug message."""
        self._log("debug", message, **kwargs)

    def info(self, message: str, **kwargs):
        """Logs an info message."""
        self._log("info", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Logs a warning message."""
        self._log("warning", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Logs an error message."""
        self._log("error", message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Logs a critical message."""
        self._log("critical", message, **kwargs)


class VerboseLogger:
    """Enables verbose logging for HTTP requests."""
    def __init__(self, enable: bool = False):
        """Initialize the verbose logger."""
        self.enable = enable

    def configure(self):
        """ Configure verbose logging for HTTP requests if enabled. """
        if not self.enable:
            return

        # Enable HTTP connection debug logging
        http_client.HTTPConnection.debuglevel = 1

        # Configure root logger
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)

        # Configure requests/urllib3 logger
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
