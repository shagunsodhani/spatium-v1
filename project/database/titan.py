#! /usr/bin/python
#---------------------------------------------------------Import Modules----------------------------------------------------------------------#

import os
from ConfigParser import ConfigParser
import math

try :
    from bulbs.config import Config, DEBUG
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from bulbs.titan import Graph, TITAN_URI
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


def connect(app_name = "spatium_titan", config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../config', 'config.cfg') ):

    '''Open database connection and return Graph object to perform database queries'''

    config=ConfigParser()
    config.read(config_path)
    username=config.get(app_name,"user")
    password=config.get(app_name,"passwd")
    config = Config(TITAN_URI, username=username, password=password)
    config.set_logger(DEBUG)
    g = Graph(config)
    return g

if __name__ == '__main__':
    connect()