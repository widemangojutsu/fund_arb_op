import os
import logging
from datetime import datetime

def setup_logging(name="HFTSystem", level=None):
    log_level = level or os.getenv("LOG_LEVEL", "INFO")
    handlers = [logging.StreamHandler()]

    if os.getenv("LOG_TO_FILE", "true").lower() == "true":
        prefix = os.getenv("LOG_FILE_PREFIX", "hft")
        filename = f'{prefix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        handlers.append(logging.FileHandler(filename))

    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
        handlers=handlers
    )
    return logging.getLogger(name)