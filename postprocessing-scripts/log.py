import logging.config
import logging.handlers
from datetime import datetime
import os
import json
import pathlib


def setup_logging(config_name):
    config_file = pathlib.Path(config_name)
    with open(config_file) as f:
        config = json.load(f)
    logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)
    return logger