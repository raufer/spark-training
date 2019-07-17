import json

from pyspark.sql import HiveContext
from app.common import util_functions


def deleteTableProducts(hc, json_config):
    db = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("products").get("name")
    if util_functions.table_exists(hc=hc, db=db, table=table):
        hc.sql("DROP TABLE IF EXISTS %s.%s" % (db, table))


def createTableProducts(hc, json_config):
    database = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("products").get("name")

    hc.sql("USE %s" % (database,))

    data = [
        ('Thin', 'Cell phone', 6000),
        ('Normal', 'Table', 1500),
        ('Mini', 'Tablet', 5500),
        ('Ultra thin', 'Cell phone', 5000),
        ('Vey thin', 'Cell phone', 6000),
        ('Big', 'Table', 2500),
        ('Bendable', 'Cell phone', 3000),
        ('Foldable', 'Cell phone', 3000),
        ('Pro', 'Table', 5400),
        ('Pro2', 'Table', 6500)
    ]
    df = hc.createDataFrame(data, ['product', 'category', 'revenue'])
    df.write.format("orc").mode("append").saveAsTable(database + "." + table)