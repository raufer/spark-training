# Exercise 4: Parallelism Level
Clusters will not be fully utilized unless the level of parallelism for each operation is high enough. 
Spark automatically sets the number of partitions of an input file according to its size and for distributed shuffles. 

It is crucial to ensure that spark applications run with the right level of parallelism. This is useful to ensure
that we are leveraging the cluster's resources and also to prevent OOM errors.

By default spark creates one partition for each block of the file in HDFS (64 / 128 MB by default).

As a rule of thumb tasks should take at least 100 ms to execute; you can ensure that this is the case by monitoring the task execution latency from the Spark Shell. If your tasks take considerably longer than that keep increasing the level of parallelism, by say 1.5, until performance stops improving.

Generically the size of the data set can be given by:
```python
M = N * V * S
```
Where `N` is the number of records, `V` the number of variables (columns) and `S` the average width in bytes of a variable. The later should take into account the types of each variable and factor that into the average calculation.

Alternatively, we can also use:
```python
number Of Megabytes = M = (N*V*W) / 1024^2
```
Where:
```python
N  =  number of records
V  =  number of variables
W  =  average width in bytes of a variable
```
The 1,0242 in the denominator rescales the results to megabytes.

We can estimate the average row size empirically, by sampling a couple of millions of rows or we can look to the schema of the data and estimate the average size based on that.

To approximate `W  =  average width in bytes of a variable` we can use:

| **Type of variable**                           | **Width**      |
|------------------------------------------------|----------------|
| Integers, −127 <= x <= 100                     | 1              |
| Integers, 32,767 <= x <= 32,740                | 2              |
| Integers, -2,147,483,647 <= x <= 2,147,483,620 | 4              |
| Floats single precision                        | 4              |
| Floats double precision                        | 8              |
| Strings                                        | maximum length |

Say that you have a 20,000 observation data set. That data set contains

| **Fields**                                     | **Size** |
|------------------------------------------------|-----------
| 1  string identifier of length 20              | 20       |
| 10  small integers (1 byte each)               | 10       |
| 4  standard integers (2 bytes each)            |  8       |
| 5  floating-point numbers (4 bytes each)       | 20       |
|------------------------------------------------|-----------
| 20  variables total                            | 58       |

Thus the average width of a variable is:
```python
W = 58/20 = 2.9  bytes
```
The size of your data set is
```python
M = 20000*20*2.9/1024^2 = 1.13 megabytes
```
This result slightly understates the size of the data set because we have not included any variable labels, value labels, or notes that you might add to the data. That does not amount to much. For instance, imagine that you added variable labels to all 20 variables and that the average length of the text of the labels was 22 characters. 

## Parallelism
The number of worker units that we have to process tasks in parallel is given by:
```python
Units = Number of Executors x Number Cores / Executor
```
An heuristic is to point each partition to have around 128 MB of data. This is due to the default HDFS block size.

So we need to dynamically calculate a parallelism factor `m`, which in turn should depend on the number of units available
and the number of records to process.
```python
m = (N . S) / (128 MB . Units)
```

## Exercise
At the beginning of each spark job, the optimum parallelism level must be calculated.

For each execution, the number of rows can vary dramatically. In order to make an efficient use of the resources we must balance computational stability and parallelism factor.

Your task is to propose and implement a generic process that will run at the start of each spark job and calculates the optimum parallelism level. It should make use of the spark arguments (e.g. executors, number of cores, etc) and also the initial loaded dataframe(s).

How can we get the row count of a list of partitions by using the Hive Metastore?
