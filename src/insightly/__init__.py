"""
@author Patrick Walsh

Insightly AI Python SDK.
This module provides a Python SDK for interacting with the Insightly AI API.


"""

from loguru import logger
import datetime
import os
from pathlib import Path

__version__ = "0.1.0"
__name__ = "Insightly"

ROOT_PATH: str = str(Path(__file__).resolve()).split("src/", maxsplit=1)[0]

date_folder: str = os.path.join(
    ROOT_PATH, "logs", datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%SZ")
)

if not os.path.exists(date_folder):
    os.makedirs(date_folder)

# configure folder for log files
logger.add(
    os.path.join(date_folder, f"{__name__}.log"),
    rotation="500 MB",
    level="DEBUG",
    retention="10 days",
)
