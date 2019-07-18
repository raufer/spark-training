import json
import os.path
import sys

from pyspark.sql import HiveContext

def load_json_config(path):
    """
    Method to load a json file into a dict object
    :param path: Path of the json file
    :return: dict object of the json file
    """
    def open_json_file():
        if os.path.isfile(path):
            with open(path) as json_file:
                return json_file.read()
        else:
            sys.exit("Config File " + path + " does not exist.")


    return json.loads(open_json_file())

def database_exists(hc, db):
    return bool([x.datadaseName for x in hc.sql("SHOW DATABASES").collect() if x.datadaseName == db])

def table_exists(hc, db, table):
    if database_exists(hc=hc, db=db):
        hc.sql("USE %s" % (db,))
        return table in hc.tableNames() 
    else: 
        False
