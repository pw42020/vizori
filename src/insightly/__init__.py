"""
@author Patrick Walsh

Insightly AI Python SDK.
This module provides a Python SDK for interacting with the Insightly AI API.


"""

import logging
import os
from pathlib import Path

ROOT_PATH: str = str(Path(__file__).resolve()).split("src/", maxsplit=1)[0]

__version__ = "0.1.0"


class ColorFormatter(logging.Formatter):
    """class that creates custom formatting for logging"""

    grey = "\033[32m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    fmt = ["[%(levelname)s] [%(asctime)s %(filename)s:%(lineno)d]", " %(message)s"]

    FORMATS = {
        logging.DEBUG: blue + fmt[0] + reset + fmt[1],
        logging.INFO: grey + fmt[0] + reset + fmt[1],
        logging.WARNING: yellow + fmt[0] + reset + fmt[1],
        logging.ERROR: red + fmt[0] + reset + fmt[1],
        logging.CRITICAL: bold_red + fmt[0] + reset + fmt[1],
    }

    def format(self, record: logging.LogRecord) -> str:
        """format the record

        Parameters
        ----------
        record : logging.LogRecord
            the record to format"""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%dT%H:%M:%SZ")
        return formatter.format(record)


class NoColorFormatter(logging.Formatter):
    """class that creates custom formatting for logging"""

    fmt = "[%(levelname)s] [%(asctime)s %(filename)s:%(lineno)d] %(message)s"

    FORMATS = {
        logging.DEBUG: fmt,
        logging.INFO: fmt,
        logging.WARNING: fmt,
        logging.ERROR: fmt,
        logging.CRITICAL: fmt,
    }

    def format(self, record: logging.LogRecord) -> str:
        """format the record

        Parameters
        ----------
        record : logging.LogRecord
            the record to format"""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%dT%H:%M:%SZ")
        return formatter.format(record)


# configure logging

# create log with 'spam_application'
log = logging.getLogger("Insightly")
log.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(ColorFormatter())

log.addHandler(ch)

# add log handler that outputs to a file
if not os.path.exists(os.path.join(ROOT_PATH, "logs")):
    os.makedirs(os.path.join(ROOT_PATH, "logs"))
fh = logging.FileHandler(os.path.join(ROOT_PATH, "logs", "insightly.log"))
fh.setLevel(logging.DEBUG)
fh.setFormatter(NoColorFormatter())
log.addHandler(fh)

log.debug("debug")
log.info("info")
log.warning("warning")
log.error("error")
log.critical("critical")
