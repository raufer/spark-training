# Exercise 1: Windows Functions
## Window Functions
Window functions significantly improve the expressiveness power of Spark. Typically, builtin functions like `round` or `abs` take values from a single row as input and generate a single return for every input row.

Other functions can be seen as *aggregate functions*, e.g. `sum`, `maximum` or `average`. These operate not only on a single row but on a group of rows and calculate a **single return value for every group**.

While these are extremely useful in practice, there is still a wide range of operations that cannot be expressed just by using them.

More specifically, there isn't a way to mix both approaches, i.e. operate on a group of rows while still returning a **single value for every input row**. This can be a huge limitation for certain type of analysis. The following are simple examples of calculations that require this capability:

- Moving average
- Cumulative sums
- Generally, every calculation that needs to access other row before the current one being processed.

Window Functions fill this gap.

These are functions that will return a value for every input row of a table. The value that is returned, however, is calculated based on a group of rows, which typically is called a *Frame*.
Note that there is nothing preventing each row of having a unique frame associated with it. This last point is what makes Window Functions extremely useful.

 
There are various **ranking functions** available:
- `row_number`: assigns unique numbers to each row within the Frame given by the *order by* clause
- `rank`: behaves like `row_number` except that "equal" rows are ranked the same
- `dense_rank`: behaves like a `rank` with no gaps

## Exercise
Consider the `products` table.

```
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").getOrCreate()

data = [
    ('Thin', 'Cell phone', 6000),
    ('Normal', 'Tablet', 1500),
    ('Mini', 'Tablet', 5500),
    ('Ultra thin', 'Cell phone', 5000),
    ('Vey thin', 'Cell phone', 6000),
    ('Big', 'Tablet', 2500),
    ('Bendable', 'Cell phone', 3000),
    ('Foldable', 'Cell phone', 3000),
    ('Pro', 'Tablet', 5400),
    ('Pro2', 'Tablet', 6500)
]

products = spark.createDataFrame(data, ['product', 'category', 'revenue'])

products.show()
```

Answer the following questions:

1. What is the best selling product in each category?
2. What are the best and second best-selling product in each category?
3. What is the difference between the revenue of each product and the best selling product in the same category of the product?
4. What is the difference between the revenue of each product and the average revenue of the category if that product?
    
    
    

