# Solutions to exercise 1: Windows Functions
ssh to a node on the cluster you are working with and start a Python Spark shell:

```bash
pyspark
```
It is supposed synthetic data is accessible. To create and delete necessary data pleas refer to the README.md on master branch.

## 1. What is the best selling product in each category?
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'spark_training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + table)

w1 = Window.partitionBy('category').orderBy(F.col('revenue').desc())
dfs1= df.withColumn('rank', F.row_number().over(w1))

dfs1.filter(F.col('rank') == 1).show()
```

## 2. What are the best and second best-selling product in each category?
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'spark_training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + table)

w2 = Window.partitionBy('category').orderBy(F.col('revenue').desc())
dfs2 = df.withColumn('rank', F.row_number().over(w2))

dfs2.filter(F.col('rank') <= 2).show()
```

## 3. What is the difference between the revenue of each product and the best selling product in the same category of the product?
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window


database = 'spark_training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + table)

w3 = Window.partitionBy('category').orderBy(F.col('revenue').desc())
dfs3 = df.withColumn('revenue_difference', F.max(F.col('revenue')).over(w3) - F.col('revenue'))

dfs3.show()
```

## 4. What is the difference between the revenue of each product and the average revenue of the category if that product?
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'spark_training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + table)

w4 = Window.partitionBy('category')
dfs4 = df.withColumn('revenue_difference', F.col('revenue') - F.avg(F.col('revenue')).over(w4))

dfs4.show()
```
 
    
    
    

