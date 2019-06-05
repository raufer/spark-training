### Solutions 1: Windows Functions

#### 1. What is the best selling product in each category?
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + 'table')

window = w = Window.partitionBy('category').orderBy(F.col('revenue').desc())
df = df.withColumn('rank', F.row_number().over(w))

answer = df.filter(F.col('rank') == 1)
answer.show()
```

#### 2. What are the best and second best-selling product in each category?

```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + 'table')

window = w = Window.partitionBy('category').orderBy(F.col('revenue').desc())
df = df.withColumn('rank', F.row_number().over(w))

answer = df.filter(F.col('rank') <= 1)
answer.show()
```

#### 3. What is the difference between the revenue of each product and the best selling product in the same category of the product?

```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + 'table')

window = w = Window.partitionBy('category').orderBy(F.col('revenue').desc())
df = df.withColumn('revenue_difference', F.max(F.col('revenue')).over(w) - F.col('revenue'))

answer = df
answer.show()
```

#### 4. What is the difference between the revenue of each product and the average revenue of the category if that product?
    
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window

database = 'training'
table = 'products'

sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

df = hc.table(database + '.' + 'table')

window = w = Window.partitionBy('category')
df = df.withColumn('revenue_difference', F.col('revenue') - F.avg(F.col('revenue')).over(w))

answer = df
answer.show()
```
 
    
    
    

