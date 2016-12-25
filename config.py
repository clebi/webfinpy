# -*- coding: utf-8 -*-

import os
import logging
import yaml

conf = None
with open(os.environ['CONFIG_FILE'], 'r') as configfile:
    conf = yaml.load(configfile)

with open(os.environ['LOGGER_FILE'], 'r') as loggerfile:
    logger_conf = yaml.load(loggerfile)
    logging.config.dictConfig(logger_conf)
