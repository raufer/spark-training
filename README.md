### Spark Training

Each exercise has its own branch.

```
git checkout <exercise>
```

Exercises:

```
* exercise-01/window-functions
* exercise-02/hash-partitioner
* exercise-03/range-partitioner
```

### Data Generation

The exercises make use of two tables. `products` and `contracts` data.

The scripts to generate artificial data for both of these tables is in `data/<table>.py`.
The values can be parameterized on the script.

Note: for the `contracts` data, we need around 200 M rows. Since we need to parallelize the data from the driver,
it must be done using several batches.
