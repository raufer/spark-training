import json

from pyspark.sql import HiveContext
from app.common import util_functions


def create_database(hc, json_config):
    hc.sql("CREATE DATABASE IF NOT EXISTS  %s" % (json_config.get("database").get("name"),))
    

def delete_database(hc, json_config):
    db = json_config.get("database").get("name")
    if util_functions.database_exists(hc=hc, db=db):
        hc.sql("DROP DATABASE IF EXISTS %s" % (db,))