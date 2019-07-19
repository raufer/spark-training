# Solution 2: Hash Partitioner
Method to label the duplicates:
```python
from pyspark import SparkContext
from pyspark import HiveContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window


hc = Hivecontext(sc)


def enum(**enums):
    """
    Helper method to easily create a `Enum`.
    Makes use of Python's `type` function acting as a metaclass,
    i.e. a factory of classes
    http://www.dabeaz.com/py3meta/
    """
    return type('Enum', (), enums)


def to_spark_column_list(arg):
    """
    Converts a string representing a column to a pypsark.sql.Column
    Does nothing if already in the desired format

    Normalizes an argument that can be a single spark column or a list of them
    """
    l = arg if isinstance(arg, list) else [arg]
    return [F.col(i) if isinstance(i, basestring) else i for i in l]


def rank_precise(partition_by, order_by, window=None):
    """
    Rank each line of a dataframe segregated by the 'parition_by' column(s)
    This version of 'rank' provides a more fine grained control over the sorting mechanism

    'order_by' defines the column by which each group is to be ordered
    a list of pairs: [(col, mode)] is expected
    in this scenario, the 'mode' argument is ignored
    >>> data = [('A', 110, 120), ('A', 110, 110), ('A', 100, 120), ('A', 100, 130)]
    >>> df = hc.createDataFrame(data, ['id', 'val1', 'val2']).show()
    +---+----+----+
    | id|val1|val2|
    +---+----+----+
    |  A| 110| 120|
    |  A| 110| 110|
    |  A| 100| 120|
    |  A| 100| 130|
    +---+----+----+
    >>> window_op = rank_precise(partition_by='id', order_by=[('val1', 'asc'), ('val2', 'desc')]))
    >>> df.withColumn('rank', window_op).show()
    +---+----+----+----+
    | id|val1|val2|rank|
    +---+----+----+----+
    |  A| 100| 130|   1|
    |  A| 100| 120|   2|
    |  A| 110| 120|   3|
    |  A| 110| 110|   4|
    +---+----+----+----+
    """
    partcols = to_spark_column_list(arg=partition_by)
    ordercols = zip(to_spark_column_list(arg=[c for c, _ in order_by]), [m for _, m in order_by])

    w = Window.partitionBy(*partcols).orderBy(
        *[c.desc() if mode.startswith('desc') else c.asc() for c, mode in ordercols])

    return F.row_number().over(w).alias('rank')
    
    
Labels = enum(
    PRIMARY='Y',
    DUPLICATE='N'
)


def labels_duplicates_by_date(df, groupcols, sortingcols, labelcol):
    """
    There are some contracts that are reported by both counterparties to a (possibly different)
    Trading Repository. These duplicated contracts are expected to have the same Trade ID, which we
    can use to flag duplicates using different methods.

    'groupcol' is the column(s) used to detect the duplicates
    'labelcol' is the output column with the label indication
    'sortingcol' is the column(s) used to decide which contracts are highest within each group, e.g. TBL.NOTIONAL_AMOUNT_1
        [(col, 'asc'/'desc')]

    We assume the timestamps come in ISO 8601 date format / UTC time format

    https://en.wikipedia.org/wiki/ISO_8601

    Execution Timestamp: ISO 8601 'yyyy-MM-dd'T'HH:mm:ss'Z' (ascending)
    Effective Date Leg 1: ISO 8601 'yyyy-MM-dd' (ascending)
    Maturity Date: ISO 8601 'yyyy-MM-dd' (descending)
    """
    groupcols = groupcols if isinstance(groupcols, list) else [groupcols]

    rank_op = rank_precise(partition_by=groupcols, order_by=sortingcols)
    df = df.withColumn('rank', rank_op)

    label_op = F.when(reduce(lambda acc, x: acc | x, [F.col(c).isNull() for c in groupcols]), F.lit(None)).when(
        F.col('rank') == 1, Labels.PRIMARY).otherwise(Labels.DUPLICATE)

    df = df.withColumn(labelcol, label_op)
    return df.select(*[c for c in df.columns if c not in ['rank']])
```

How to avoid NULL trade IDs causing problems. We salt all of the rows that have a NULL Trade ID.

```python

def random_hex_string(token='', n_bits=256):
    """
    Create a column that contains random values
    Token is a prefix to post-add to the hash function output.
    This can work as a watermark to identify random values

    >>> token = '<TOKEN>'
    >>> op = random_hex_string(token=token)
    >>> df.withColumn('rand', op).show()
    .+---+----+--------------------+
    | id|name|                rand|
    +---+----+--------------------+
    |  0| Bob|<TOKEN>06de179159...|
    |  1| Sue|<TOKEN>991789120b...|
    |  2| Tom|<TOKEN>07f13393c3...|
    +---+----+--------------------+
    """
    to_hash = F.monotonically_increasing_id().cast(T.StringType())
    hex_string = F.sha2(to_hash, n_bits)
    return F.concat(F.lit(token), hex_string)
    

from pyspark.sql import types as T
    
tmp_group_col = 'trade_id_temp'

salted_trade_id = F.when(F.col('trade_id').isNull() | (F.col('trade_id') == ''), random_hex_string()).otherwise(
    F.col('trade_id'))

df = df.withColumn(tmp_group_col, salted_trade_id)

groupcols = [tmp_group_col]

sortingcols = [
    ('execution_timestamp', 'asc'),
    ('effective_date', 'asc'),
    ('maturity_date', 'desc')
]

labels_duplicates_by_date(
    df=df,
    groupcols=groupcols,
    sortingcols=sortingcols,
    labelcol=TBL.DUP_METHOD_2
).show()
```
