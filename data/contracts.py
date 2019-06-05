import random

from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

database = 'training'
table = 'contracts'

batches = 10
batch_size = 1e6

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
