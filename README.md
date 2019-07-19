# Exercise 3: Range Partitioner

When spark is performing a sorting operation, it makes use a Range Partitioner.

Instead of basing the decision of partition allocation based on the hash value of the keys, it creates `n_partitions` buckets with ordered numerical ranges and then places each row on the corresponding bucket. So the range partitioner uses a range to distribute to the respective partitions the keys that fall within a range. This method is suitable where there’s a natural ordering in the keys and the keys are non negative.  

**Note:** When sorting a string column, the range partitioner uses a numerical representation of the string.

To actually build the buckets, an extra `count()` operation will be performed under the hood (try to see this on the spark UI). You can also check this in the pyspark source code:

`python/pyspark/rdd.py`
```python
rddSize = self.count()
if not rddSize:
    return self  # empty RDD
maxSampleSize = numPartitions * 20.0  # constant from Spark's RangePartitioner
fraction = min(maxSampleSize / max(rddSize, 1), 1.0)
samples = self.sample(False, fraction, 1).map(lambda kv: kv[0]).collect()
samples = sorted(samples, key=keyfunc)

# we have numPartitions many parts but one of the them has
# an implicit boundary
bounds = [samples[int(len(samples) * (i + 1) / numPartitions)]
          for i in range(0, numPartitions - 1)]

def rangePartitioner(k):
    p = bisect.bisect_left(bounds, keyfunc(k))
    if ascending:
        return p
    else:
        return numPartitions - 1 - p
```

## Exercise

Sort the `contracts` data by Trade ID efficiently, i.e. prevent the long running tasks problem.
You need to find a way of having buckets (at sort time) with more spread out boundaries, while preserving a correct ordering.
