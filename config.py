# -*- coding: utf-8 -*-

import os
import yaml

conf = None
with open(os.environ['CONFIG_FILE'], 'r') as configfile:
    conf = yaml.load(configfile)
