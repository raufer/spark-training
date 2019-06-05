### Solution 5: Sizing a spark session

Suppose we have a 10 node cluster with the following specs:

```
10 Nodes
16 cores per Node
64GB RAM per Node
```

- Based on the recommendations mentioned above, Let's assign 5 core per executors => --executor-cores = 5 (for good HDFS throughput)

- Leave 1 core per node for Hadoop/Yarn daemons => Num cores available per node = 16-1 = 15
- Total available of cores in cluster = 15 x 10 = 150
- Number of available executors = (total cores/num-cores-per-executor) = 150/5 = 30
- Leaving 1 executor for ApplicationManager => --num-executors = 29
- Number of executors per node = 30/10 = 3
- Memory per executor = 64GB/3 = 21GB
- Counting off heap overhead = 7% of 21GB = 3GB. So, actual --executor-memory = 21 - 3 = 18GB

**Recommended configuration:** 29 executors, 18GB memory each and 5 cores each.



