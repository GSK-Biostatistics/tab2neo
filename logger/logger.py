"""
Custom logger. Level INFO by default, can be changed to DEBUG if CLD is ran with --debug option.
"""

import logging
import os
import colorlog
from pathlib import Path
from time import gmtime, strftime

now = strftime("%Y-%m-%d_%H-%M-%S", gmtime())

logger = logging.getLogger("cld")
logger.setLevel(logging.INFO)

Path(os.path.join("logs", "logger")).mkdir(parents=True, exist_ok=True)
file_handler = logging.FileHandler(os.path.join("logs", "logger", f"main_{now}.log"))

file_handler.setFormatter(logging.Formatter("%(asctime)s    %(levelname)s    %(message)s"))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

stream_handler = colorlog.StreamHandler()
stream_handler.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s    %(name)s    %(levelname)s    %(message)s",
        log_colors={
            "DEBUG": "white",
            "INFO": "white",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info("-------------------------------   Loaded CLD Logger    -------------------------------")
