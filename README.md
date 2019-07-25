# Solution 4: Parallelism Level

Let's assume we have empirically estimated the average row size to be `800 bytes` (over a sample of 150 M records).

Given a dataframe, we must first know the number of rows it has. We can make use of the Hive metastore information.


```python
import math
from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)


AVG_RECORD_SIZE = 800
DESIRED_PARTITION_SIZE = 128e6

def dynamic_parallelism_factor(hc, n):
    """
    Since the number of records for each file can have huge variations we need to be able
    to partition the data dynamically, to prevent OOM errors.
    We increase the multiplier for the number of tasks every N records.

    'n' is the number of records.

    Additionally we need to do the same for the default number of partitions that spark creates
    after a shuffle stage.

    An heuristic is to point each partition to have around 128 MB of data.
    DESIRED_PARTITION_SIZE is the desired size of each partition that spark processes in each tasks.
    Might be different from the actual block that is written into HDFS.
    """

    factor = (n * AVG_RECORD_SIZE) / DESIRED_PARTITION_SIZE
    return max(int(math.ceil(factor)), 1)

count = df.count()

print int(dynamic_parallelism_factor(hc, count))
```
