### Exercise 2: Hash Partitioner

Spark uses different partitioning schemes for various types of RDDs and operations. 
In a case of using `parallelize()` data is evenly distributed between partitions using their indices 
(no partitioning scheme is used).

    So If there is no partitioner the partitioning is not based upon characteristic 
    of data but distribution is random and uniformed across nodes.
    
Each RDD also possesses information about partitioning schema 
(it can be invoked explicitly or derived via some transformations). 

```python
from pyspark import SparkContext


sc = SparkContext.getOrCreate()

# The default data used for calculations
nums = range(0, 10)
print(nums)

rdd = sc.parallelize(nums)

print("Default parallelism: %s" % str(sc.defaultParallelism))
print("Number of partitions: %s" % str(rdd.getNumPartitions()))
print("Partitioner: %s" % str(rdd.partitioner))
print("Partitions structure: %s" % str(rdd.glom().collect()))
```

Output

```
Default parallelism: 2
Number of partitions: 2
Partitioner: None
Partitions structure: [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```

The data was distributed across two partitions and each will be executed in a separate thread.

What will happen when the number of partitions exceeds the number of data records?


```python
from pyspark import SparkContext

rdd = sc.parallelize(nums, 15)

print("Number of partitions: %s" % str(rdd.getNumPartitions()))
print("Partitioner: %s" % str(rdd.partitioner))
print("Partitions structure: %s" % str(rdd.glom().collect()))
```

Output

```bazaar
Number of partitions: 15
Partitioner: None
Partitions structure: [[], [0], [1], [], [2], [3], [], [4], [5], [], [6], [7], [], [8], [9]]
```

You can see that Spark created requested a number of partitions but most of them are empty. 
This is bad because the time needed to prepare a new thread for processing data (one element) 
is significantly greater than processing time itself (you can analyze it in Spark UI).

### `coalesce()` and `repartition()`

`coalesce()` and `repartition()` transformations are used for changing the number of partitions in the RDD.

`repartition()` is calling `coalesce()` with explicit shuffling.

The rules for using are as follows:

    - if you are increasing the number of partitions use repartition()(performing full shuffle),
    
    - if you are decreasing the number of partitions use coalesce() (minimizes shuffles)
    
```python
from pyspark.sql import HiveContext

hc = HiveContext(sc)

nums_df = hc.createDataFrame(list(range(0, 10)), ['num'])

print("Number of partitions: %s"  % str(nums_df.rdd.getNumPartitions()))
print("Partitions structure: %s" % str(nums_df.rdd.glom().collect()))

nums_df = nums_df.repartition(4)

print("Number of partitions: %s" % str(nums_df.rdd.getNumPartitions()))
print("Partitions structure: %s" % str(nums_df.rdd.glom().collect()))
```

Output

```
Number of partitions: 2
Partitions structure: [[Row(num=0), Row(num=1), Row(num=2), Row(num=3), Row(num=4)], [Row(num=5), Row(num=6), Row(num=7), Row(num=8), Row(num=9)]]
Number of partitions: 4
Partitions structure: [[Row(num=1), Row(num=6)], [Row(num=2), Row(num=7)], [Row(num=3), Row(num=8)], [Row(num=0), Row(num=4), Row(num=5), Row(num=9)]]
```

### Hash Partitioner

Everytime that we need to perform an operation over a group, e.g. using a `groupBy` or a `window function`, spark will distribute the 
data among the different partitions using the `HashPartitioner`.

To decide on which partition a particular raw belongs, the following operation is done:

```
partition = key.hashCode() % numPartitions
```

So rows, whose group have the same `hashCode`, will unavoidably fall into the same partition. This can
lead to a data skew problem, where we have some partitions significantly bigger that the others.

The skew increases the likelihood of a memory error. It also means the resources are not being efficiently used,
since the job running time will be dominated by the `long running tasks`, e.g. a spark job that gets stuck at the last task `99/100`.

### Exercise

Some contracts have been reported more than once, which leads to duplicates. To analyze the data we need to pick one trade among each
group of duplicated trades. Your task is to run a spark job that flags the valid contracts.

If a contract if its `trade_id` is unique amid the rows or, in the case it belongs to a group of duplicates, ranks first given
the following criteria:

    Execution Timestamp: ISO 8601 'yyyy-MM-dd'T'HH:mm:ss'Z' (ascending)
    Effective Date Leg 1: ISO 8601 'yyyy-MM-dd' (ascending)
    Maturity Date: ISO 8601 'yyyy-MM-dd' (descending)
    
Create a new column `valid` where valid contracts have `Y` and duplicated contracts have `N`

**Note:** The `contracts` table has a considerable amount of rows with a NULL `trade id`. 
How can we make sure that we won't have memory problems? You should find a way of preventing all of these rows falling into
the same partition (as given by the above formula)



