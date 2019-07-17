# Spark Training
Spark Training project is a series of exercises design for intermediary Spark users. It is recommended for students with at least one year practical experience running Spark on Hadoop systems.

## Requisites
Access to a Hadoop system with Spark 1.6 or greater.

## Usage
After cloning Spark Training navigate to its folder, and lis all branches:
```bash
$ git branch
  exercise-01/window-functions
  exercise-02/hash-partitioner
  exercise-03/range-partitioner
  exercise-04/parallelism-level
  exercise-05/sizing-a-session
* master
  solution-01/window-functions
  solution-02/hash-partitioner
  solution-03/range-partitioner
  solution-04/parallelism-level
  solution-05/sizing-a-session
```
There are 5 exercises and the corresponding solutions. To navigate to a selected exercise/solution:
```bash
$ git checkout <branch-name>
```
## Data Generation
Exercises make use of two tables: `products` and `contracts` data.

Data can be generated launching Spark jobs running scripts in scripts folder, respectively for creation and deleting:

```bash
$ bash scripts/spark_submit_create_data.sh
$ bash scripts/spark_submit_delete_data.sh
```

The scripts are parametrized with the json file `app/data/resources/config.json`.

**Note**: for the `contracts` data around 200 M rows are necessary. To parallelize the data from the driver data must be generated in multiple batches.

## Authors
*   [Ricardo Miranda](https://github.com/ricardomiranda)
*   [Raul Ferreira](https://github.com/raufer) - This repository is a fork from Raul Ferreira's [Spark Training](https://github.com/raufer/spark-training)

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
