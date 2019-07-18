import json

from pyspark.sql import HiveContext
from app.common import util_functions


def deleteTableContracts(hc, json_config):
    db = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("contracts").get("name")
    if util_functions.table_exists(hc=hc, db=db, table=table):
        hc.sql("DROP TABLE IF EXISTS %s.%s" % (db, table))


def createTableContracts(hc, json_config):
    database = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("contracts").get("name")

    hc.sql("USE %s" % (database,))

    data = [
        ('Thin', 'Cell phone', 6000),
        ('Normal', 'Tablet', 1500),
        ('Mini', 'Tablet', 5500),
        ('Ultra thin', 'Cell phone', 5000),
        ('Vey thin', 'Cell phone', 6000),
        ('Big', 'Tablet', 2500),
        ('Bendable', 'Cell phone', 3000),
        ('Foldable', 'Cell phone', 3000),
        ('Pro', 'Tablet', 5400),
        ('Pro2', 'Tablet', 6500)
    ]
    df = hc.createDataFrame(data, ['product', 'category', 'revenue'])
    df.write.format("orc").mode("append").saveAsTable(database + "." + table)
