# Exercise 5: Sizing a spark session
When allocating resources to a spark application, we should keep some things in mind:
- **Hadoop/Yarn/OS Daemons**. When we run spark application using a cluster manager like Yarn, there will be several daemons that will run in the background like NameNode, Secondary NameNode, DataNode, JobTracker and TaskTracker. So, while specifying num-executors, we need to make sure that we leave aside enough cores (~1 core per node) for these daemons to run smoothly.
- **Yarn ApplicationMaster (AM)**. ApplicationMaster is responsible for negotiating resources from the ResourceManager and working with the NodeManagers to execute and monitor the containers and their resource consumption. If we are running spark on yarn, then we need to budget in the resources that AM would need (~1024MB and 1 Executor).
- **HDFS Throughput**: HDFS client has trouble with tons of concurrent threads. It was observed that HDFS achieves full write throughput with ~5 tasks per executor. So it’s good to keep the number of cores per executor below that number.

 Also note that:
```python
Full memory requested to yarn per executor =
          spark-executor-memory + spark.yarn.executor.memoryOverhead.

 spark.yarn.executor.memoryOverhead =
        	Max(384MB, 7% of spark.executor-memory)
```
So, if we request 20GB per executor, AM will actually get 20GB + memoryOverhead = 20 + 7% of 20GB = ~23GB memory for us.

Additional considerations:
- Running executors with too much memory often results in excessive garbage collection delays.
- Running tiny executors (with a single core and just enough memory needed to run a single task, for example) throws away the benefits that come from running multiple tasks in a single JVM.
- Shared/cached variables like broadcast variables and accumulators are be replicated in each executor.
- Each executor has a JVM.
- An executor with more than one core can run multiple tasks in the same JVM.

## Exercise
Suppose we have a 10 node cluster with the following specs:
```
10 Nodes
16 cores per Node
64GB RAM per Node
```
How can we size a single session to make the best use of the full cluster? 
Propose and justify a possible session configuration.
