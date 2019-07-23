# Solution 3: Range Partitioner

To prevent long running tasks we need to ensure that we do not have a high concentration of repeated values (on the sorting column)

```python
from pyspark.sql import functions as F

def salt_column(col, sep='', n_bits=256):
    """
    Adds random data as a prefix to the input 'col'.
    Returns an string column with the new logic

    'n_bits' indicates the desired bit length of the salt to add
    'sep' is the string to use as a separator, e.g. col + sep + salt

    We compute the hash based on 'col' concatenated with an integer that is guaranteed
    to be unique throughout the dataset

    >>> df = sql_context.createDataFrame([(0, 'ID0'), (1, 'ID2'), (2, 'ID0'), (3, 'ID1')], ['index', 'id'])
    >>> df.show()
    +-----+---+
    | index | id |
    +-----+---+
    | 0 | ID0 |
    | 1 | ID2 |
    | 2 | ID0 |
    | 3 | ID1 |
    +-----+---+
    >>> df2 = df.withColumn('id', salt_column(col='id', n_bits=256))
    >>> df2.sort('id').show()
    +-----+--------------------+
    |index|                  id|
    +-----+--------------------+
    |    2|ID068defa6e315e62...|
    |    0|ID083ab9fb78995f2...|
    |    3|ID1a9727997291822...|
    |    1|ID2ee85ac0533215d...|
    +-----+--------------------+
    """
    unique = F.concat(F.col(col), F.monotonically_increasing_id())
    concatenated = F.concat_ws(sep, F.col(col), F.sha2(unique, n_bits))
    return concatenated


sortcols = [salt_column('trade_id', n_bits=256)]

df = df.sort(sortcols)
```
