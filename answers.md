Why are the credentials stored in a Secret and not in the ConfigMap?
=> for them not to be leaked anywhere, a configmap is for non sensitive informations and is clear text that anyone that can access the configuration could read it , sensitive informations needs to be crypted 

The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?
=> If the job is executed again tomoroww it would fail. Once the secret has expired, the s3 backend rejects it.
A production platform would provide credentials to a job by rotating automaticaly the credentials and keeping the kubernetes secret updated

How would you turn this Job into a daily ingestion?
=> By scheduling a run everyday at the same time and being carefull to overlapping runs


The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?
=> the credentials are stored in a file of the home directory: any program running as my user can read them, and the file could be copied, shared or committed by mistake. On Onyxia the risk is limited because the credentials are temporary (they expire) and the home directory belongs to my personal service, in my own namespace, which other users cannot access


The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?
=> by giving the job its own credentials with only the permissions it needs, for example write access to the bronze/ prefix only, injected through a secret used by this Job only, or through a dedicated service account with short living credentials


The dataset contains 50 users, each one with at least one order, and 2829 orders with unique identifiers. Why does approx_unique return 40 and 3167?
=>approx_unique does not count exactly: it uses an approximation algorithm which estimates the number of distinct values with little memory and time. The result is an estimate with an error margin, which can be large on small datasets.


The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?
=> the type can be wrongly inferred: for example a column detected as an integer in the sample but containing text or decimals further in the file. The read then fails or the values are badly converted. The fix is to declare the types explicitly or to increase the sample size


Exercises
Write the queries answering the following questions:

What is the average number of orders per user, and the average quantity per order?
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

For each user, what is the date of their first and last order, and the number of days between them?
SELECT
  u.username,
  u.name,
  min(o.date)::DATE AS first_order,
  max(o.date)::DATE AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY first_order;

Which hour of the day has the highest quantity sold? Use the hour function.
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 1;

For each product, what is the month-over-month variation of the quantity sold, in percent? Use the lag window function.
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
)
SELECT
  product,
  strftime(month, '%Y-%m') AS month_label,
  quantity,
  lag(quantity) OVER w AS previous_quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

Which users ordered every product at least once?
SELECT u.username, u.name, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;


Why is the uuid column barely compressed?
=> uuids are random and all different: there is no repetition to exploit, so neither dictionary encoding nor compression can reduce their size much

Why is the compressed size of some columns larger than their uncompressed size?
=> when the data is already compact (binary integers and timestamps, dictionary-encoded values), the compression cannot reduce it further, and the compression format adds a small overhead (headers, metadata), so the result is slightly larger
Its because it's already stored in the minimal size 


Why is partitioning by uuid a bad idea? Refer to the limitations of object storage presented in the previous module.
=>it creates one directory and one tiny file per order. On object storage each file costs at least one HTTP request, listing is slow and there is no real directory, so thousands of small files make reads and writes very slow and expensive (the small files problem). The partition column must have few distinct values


Which partition column would you choose for a dataset of orders growing every day?
=> the order date (for example the day, or the month if the daily volume is small): each day of data is written in a new partition without touching the previous ones, and queries on a period only read the matching partitions


How many row groups does the file contain, and how many are skipped by the filter date >= '2100-01-01'?
=>the file contains 3 row groups. With the dataset I generated, the dates go up to about 2048, so 3 row groups are skipped: their max statistic is before 2100, so the engine knows they cannot match the filter without reading them.


The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?
=> every row group would contain dates from the whole period, so its min/max statistics would cover the whole range. No row group could be skipped and the engine would have to read the date column of every row group


Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?
=>the network part comes from the volume downloaded: the CSV query downloads the whole file (28,2 MiB) while the Parquet query only downloads the footer and two columns (203,5 KiB). The parsing part comes from the CPU work, the csv text must be read character by character and converted into types, while Parquet values are already typed and stored in binary

# Lab dbt - medallion architecture

## Bronze layer

The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?
=> The bug is in the code, not in the stored data. The model is corrected and `dbt build` replays the whole history from the untouched CSV files, so six months of silver data are rebuilt correctly. If the ingestion had overwritten the raw files with cleaned ones, the original values would be gone and the error would be unrecoverable.

The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?
=> Parquet, partitioned by date in a Hive layout such as `orders/date=2026-11-05/`. Parquet is columnar and compressed, so a query reads only the columns it needs and benefits from predicate pushdown on row group statistics; the partitioning lets the engine prune whole directories, so a query on one month reads 30 directories instead of parsing years of CSV.

## Build

The silver models are views. What happens when a view is queried, and when is it preferable to a table?
=> A view stores the SQL text in the catalog. Each query re-executes the transformation, which here means re-reading the bronze CSV files from S3 every time. A view is preferable when the result must always reflect the current source, when storage is constrained, or when the transformation is cheap compared to the cost of keeping a copy in sync. A table is preferable when the transformation is expensive and the result is read often.

`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?
=> The canonical form is 32 hexadecimal characters plus 4 hyphens, so 36 bytes in UTF-8, plus the string overhead of the engine. That is 2.25 times the binary form, which is why the `uuid` column of the DuckDB lab compressed so poorly: hexadecimal text of random values has high entropy and nothing to dictionary-encode.

## Data tests

Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?
=> For the age dashboard the birthdate is the measured quantity, so a wrong birthdate biases the metric itself. The invalid values must be set to NULL and excluded from the average, and the share of users without a usable birthdate reported next to it. For the sales per region dashboard the birthdate is irrelevant; dropping those users would remove real orders from the regional totals. A warning on the test is enough there.

The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?
=> A test joining the first three digits of the ZIP code to a reference table of the prefix ranges allocated to each state, and returning the rows with no match. The reference data is the USPS ZIP code allocation. Implemented in exercise 4 as the `states` seed and the `assert_zip_code_matches_state` test, which flags 49 of the 50 generated users.

## Exercises

1. Add a cleaned column
=> `username_normalized` added to `stg_users` as `lower(trim(username))`, documented in `silver.yml` and covered by `unique` and `not_null`. `unique` is the meaningful test here: it proves the normalization does not collapse two distinct usernames into one.

2. Improve order validation
=> `product` and `user_id` are already covered by the generic `accepted_values` and `relationships` tests. Strict positivity of `quantity` has no generic equivalent in dbt core, since `accepted_values` only checks a closed list, so it is written as the singular test `assert_orders_quantity_positive`. Installing `dbt_utils` would allow the generic `expression_is_true` instead, at the cost of a package dependency.

3. Clean invalid birthdates
=> `stg_users` now joins the first order date of each user, computed from `stg_orders`, and exposes `birthdate_is_valid`. The birthdate is replaced with NULL when it is later than that first order. A user with no order cannot be invalidated. 5 of the 50 users are affected, which is the source of the 288 rows that `assert_orders_after_user_birth` used to return. That test now passes without the `severity='warn'` configuration, which has been removed.

4. Detect invalid ZIP codes
=> Implemented as the `states` seed (state code, 3-digit prefix range) and the singular test `assert_zip_code_matches_state`. 49 of the 50 users fail, which is expected since the generator pairs a state with a random ZIP code.

Where should this reference data come from?
=> From the USPS, which allocates the ZIP prefixes. The seed committed here was built from a public prefix list and should be replaced by the USPS source before being trusted in production.

Should it be stored as a dbt seed?
=> Yes at this size. The table is small, changes rarely, and must be versioned together with the test that depends on it. A large or frequently updated mapping, such as a full 5-digit ZIP file, belongs in a source table loaded by the ingestion process, not in a CSV committed to git.

Should the validation happen in the silver layer?
=> The detection belongs to the tests, not to the transformation. The silver layer must not invent a state or a ZIP code: neither of the two values is more trustworthy than the other, so correcting one of them would fabricate data. The severity is a warning because the defect is upstream and cannot be fixed here.

What happens when the reference data changes?
=> The seed is versioned, so `dbt seed` reloads it and the tests are replayed. A redrawn range can turn rows that used to pass into failures, including historical rows that were correct when they were ingested. A production seed should therefore carry a validity period rather than a single current state.

5. Materialization experiment
=> Measured on this dataset: the database file holds 274432 bytes with views and 798720 bytes with tables, roughly three times more. Build times were 12.8 s and 15.7 s, which is not a meaningful difference at this scale since both runs re-read the bronze files during the tests. The configuration has been restored to `view`.

Where is the data stored?
=> With a view, only the SQL text in the DuckDB catalog; the rows stay in the CSV files on S3. With a table, the rows are copied into the DuckDB file.

When is the transformation executed?
=> At query time for a view, at build time for a table.

What happens when the underlying CSV changes?
=> The view reflects the change at the next query. The table keeps the state of the last `dbt run` and is stale until the next build.

Which approach requires more storage?
=> The table.

Which approach avoids re-reading the source files for every query?
=> The table.

## Notes on the lab instructions

Two blocks of the lab do not work as written on a service that has been running for a while.

The `CREATE PERSISTENT SECRET` block of the DuckDB lab produces `InvalidAccessKeyId` even after the AWS profile script of the Data Storage tab has been run. The `AWS_*` environment variables injected into the container at service startup come first in DuckDB's default credential chain and are not refreshed by that script, so the secret replays expired credentials while the AWS CLI works. Adding `CHAIN 'config'` and `PROFILE 'default'` to the secret forces it to read `~/.aws` and fixes it.

The `relationships` test declared on `stg_users.state` points to `ref('states')`, a model that does not exist anywhere in the lab. dbt does not fail: it parses the test and silently marks it as disabled, so it never runs. That is why the expected output of the lab counts 14 tests and not 15. The seed added in exercise 4 is named `states` with a `code` column, which makes the test run again; a full re-parse with `--no-partial-parse` is needed the first time, because the disabled state is cached by partial parsing.


# Lab Spark RDD

The lab is written for the interactive `pyspark` shell. It was reproduced as a single script, `lab_rdd.py`, run with `spark-submit --master 'local[*]'`, so that every figure quoted below is reproducible.

## RDD creation and partitions

What decides the number of partitions `textFile()` picks by default, for a file this size?
=> `textFile()` asks the Hadoop input format for splits, with `minPartitions` defaulting to `min(defaultParallelism, 2)`. A file smaller than one split cannot be divided further, so the floor wins. Measured here: `defaultParallelism` is 2, and both `users.csv` (7 KB) and `orders.csv` (324 KB) give 2 partitions. For a file large enough to exceed the split size, the block size would decide instead.

`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?
=> `repartition(n)` always shuffles; it is `coalesce(n, shuffle=True)`. `coalesce(n)` only merges existing partitions on the same executor and therefore cannot increase the count, which is why it is cheap: it adds a narrow dependency instead of a stage boundary.

## Lazy evaluation and lineage

Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?
=> Yes, a new job with a new stage. `orders_data` is not persisted, so the RDD holds a lineage, not data: the second action replays `textFile` and `filter` from the file. Measured 0.718 s then 0.268 s; the second run is faster only because the operating system page cache still holds the file, not because Spark reused anything.

At which point in this notebook would `.cache()` change that answer?
=> On `orders_data`, before the first `count()`. The first action would then materialize the partitions in memory and the second would read them, the stage appearing as skipped in the UI. Calling `.cache()` after the first count would still require one more full computation to populate the cache.

## The cost of no schema: the multiline address

Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?
=> The CSV reader keeps the quoting state across physical line breaks: a newline inside a quoted field is part of the value, and the record ends only at an unquoted line break. `textFile()` uses a byte-oriented line reader that knows nothing about quoting and cuts at every `\n`. Measured: 101 raw lines for 50 users, one header and two lines per record.

In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?
=> One record per line, and the line delimiter must never occur inside a value. The format must also be splittable at those boundaries so that a split taken anywhere in the file resumes on a record boundary. JSON Lines satisfies this; general CSV does not, and neither does pretty-printed JSON or XML.

## Manual parsing of orders

What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?
=> `line.split(",")` would return six fields and the five-name unpacking would raise `ValueError` in the executor. The task fails, is retried, and the job fails. It happens at the first action that touches the offending partition, not when the RDD is defined, and nothing in the RDD API offers a quarantine for the bad row.

Where does the DataFrame API perform the equivalent check, and when?
=> In the CSV reader, at read time, against the declared or inferred schema. The `mode` option decides the outcome: `PERMISSIVE` fills the row with nulls and can keep the raw text in `_corrupt_record`, `DROPMALFORMED` discards it, `FAILFAST` fails the job. The decision is made once, declaratively, instead of being scattered in a Python function.

## Key-value RDDs

`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?
=> `groupByKey` shuffles every individual value and only groups them once they have crossed the network, so the shuffle carries one record per order. `reduceByKey` runs the combiner on each partition first and shuffles one partial sum per key and per partition. Here that is 2829 values against at most 50 keys times 2 partitions, a reduction of more than an order of magnitude in shuffle volume. `groupByKey` also materializes the whole group for one key on a single executor, which is what makes it fail on a skewed key.

For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?
=> Those that are not an associative and commutative fold over the value type, that is the ones needing the whole group at once: an exact median or any other order statistic, an exact distinct count, a computation whose result depends on the order of the values. In practice `aggregateByKey` and `combineByKey` cover most of these with an accumulator of a different type than the values, so the genuinely unavoidable case is narrow, the exact quantile being the usual example.

## Manual deduplication

`partitionBy()` in the window function shuffles rows so every row of a given `order_id` lands on the same partition. What plays that role here?
=> `reduceByKey` itself. It partitions by key with a `HashPartitioner` before applying the combiner, which is the same shuffle, expressed through a different API. Measured: 2829 rows before and after, the generator producing no duplicate.

Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?
=> The window function. It says "keep the first row per `order_id` ordered by `ordered_at` descending". The `reduceByKey` says how to fold two rows into one and leaves the reader to deduce that this amounts to keeping the most recent. It also hides an assumption: `most_recent` compares `date` as a string, which is only correct because the format is ISO 8601 and therefore lexicographically ordered.

## Joining orders with users

What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?
=> Both would have to carry the same partitioner, same class and same number of partitions, for instance after `partitionBy(HashPartitioner(n))` on each and a persist. Spark then sees a one-to-one dependency and joins partition by partition without a stage boundary. In practice only one side is usually co-partitioned, and the other is still shuffled.

Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?
=> The order is kept, with `None` in place of the username. With `join()` it disappears without a trace, which is the dangerous behaviour: a referential integrity failure silently reduces the row count. Measured in exercise 2: 2829 rows with the inner join against 2830 with the left outer join, and a single orphan.

`enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset much larger than this one?
=> `collect()` brings every row back into the driver's heap. On a large dataset the driver runs out of memory, and before that the whole result crosses the network to one JVM, which also serializes a distributed computation into a single point. `take(n)` for inspection, or a write to storage for the full result, are the safe forms.

## Exercises

1. Rewrite `user_quantity` with `aggregateByKey`
=> `aggregateByKey((0, 0), ...)` accumulates `(order_count, total_quantity)` in one pass, the average following by `mapValues`. The totals match those of `reduceByKey`. The point of `aggregateByKey` is that the accumulator type differs from the value type, which `reduceByKey` cannot express.

2. Inject a fake `user_id` and redo the `leftOuterJoin()`
=> One order was added with the user `00000000-0000-0000-0000-000000000000`. The inner join returns 2829 rows, the left outer join 2830, and filtering on a null username isolates exactly 1 orphan.

3. Reimplement `user_quantity` with the DataFrame API
=> `orders_df.groupBy("user_uuid").sum("quantity")` replaces the parsing function, the `map` and the `reduceByKey`, about twenty lines reduced to one. The plans are not comparable in kind: `toDebugString` lists the RDD lineage, an order of execution already fixed by the code, while `explain` shows a plan Catalyst built and optimized. The physical plan is a partial `HashAggregate`, then an `Exchange hashpartitioning(user_uuid, 200)`, then the final `HashAggregate`. The partial aggregation before the exchange is exactly the map-side combine that `reduceByKey` performs and that `groupByKey` omits: the optimizer applies it on its own, which is the real argument for the DataFrame API.

4. Time the counts with and without `.cache()`
=> Measured on this dataset: uncached 0.154 s then 0.293 s, cached 0.251 s then 0.157 s; for the aggregation, 0.287 s then 0.136 s uncached against 0.283 s then 0.189 s cached. These differences are noise. The dataset is 324 KB and already in the page cache, so the cost of serializing the partitions into the block manager is of the same order as re-reading the file. The Storage tab confirms the mechanism, the cached RDD appearing with `Memory Serialized 1x Replicated` while the other has no storage level. `cache()` pays off when recomputing the lineage is expensive, that is a wide transformation, a remote read or a costly parse, which is not the case here.


# Lab Spark DataFrames - bronze to silver

The lab is written for the interactive `pyspark` shell. It was reproduced as a single script, `lab_silver_spark.py`, run with `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, in 2 min 2 s. Environment: Spark 4.1.1, Scala 2.13.17, `hadoop-aws` 3.4.2.

## SparkSession with S3 access

The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?
=> The session is built with the values of `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` read once, at startup, and copied into the Hadoop configuration. A restart issues new credentials in the environment but the running JVM keeps the old ones, so the next S3A call fails with a 403 or an expired token. Nothing recovers on its own: the session has to be recreated. A production setup would use a credentials provider that refreshes itself, not literal keys frozen in the configuration.

Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?
=> `SimpleAWSCredentialsProvider` only sends the access key and the secret key. Onyxia issues STS credentials, which are only valid when presented together with the session token: without it the server sees an access key it does not recognise as a standing one and rejects the request. `TemporaryAWSCredentialsProvider` is the provider that reads `fs.s3a.session.token` and adds it to the signature. The same mechanism explains the `InvalidAccessKeyId` met in the DuckDB lab.

## Silver layer and Spark SQL

Explain the output of the code that was just run.
=> The query groups the silver users by state and counts, per state, the rows whose `city` is NULL. The states with a missing city are the military ones, `AE`, `AA` and `AP`: their second address line has the form `DPO AP 09617`, with no comma and no city, so the first regular expression finds no match. For every other state the count of missing cities is zero, which is what validates the parsing.

Compare the DataFrame and SQL cells above. Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?
=> SQL for the one-off exploration: it is shorter, every analyst reads it, and it needs no object to be built beforehand. The DataFrame API for a shared model: the transformation is composed of named Python values, can be factored into functions, unit tested, and typed. The decisive argument is not readability but reuse, since both compile to the same Catalyst plan, as the two cells of this lab verify by returning the same count and the same columns.

`regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same behavior silently produce a wrong result if left unhandled?
=> On `state` and `zip_code`, extracted by the same function and left unguarded in the lab. A military address matches them, so nothing shows here, but a malformed address would produce an empty string rather than a NULL. The consequences are the same as for `city`: an empty string forms a group of its own in a `groupBy`, is counted by `count(col)`, passes a `not_null` test, and joins against nothing in a reference table. The rule is that any `regexp_extract` whose match is not guaranteed must be wrapped in the same `when(col == "", None)`.

## Write the silver layer as Parquet

List the objects written under `silver/users.parquet/`. How many part-files are there, and why?
=> One part-file, plus the `_SUCCESS` marker. Spark writes one file per partition of the DataFrame, and `stg_users` has a single partition: the source CSV is 7 KB, far below one split, and no shuffle has repartitioned it since. `stg_orders` also ends with one file even though the window function shuffled it, because the write happens after the filter on `row_number`.

`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?
=> The parallelism of the write. All the data must pass through a single task, therefore a single executor, which becomes the bottleneck and may run out of memory. The reader loses as well: a single large file can still be split by Parquet row groups, but the write no longer scales. The correct way to limit the number of small files is to size the partitions, with `repartition(n)` on a sensible key, not to collapse everything to one.

## Write the silver layer as Iceberg tables

Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?
=> The Parquet directory holds only data: part-files and a `_SUCCESS` marker, with no description of the whole. The Iceberg table separates the two. `data/` holds the Parquet files, here two of them, 84 KB for the initial write and 1.9 KB for the row appended in exercise 4. `metadata/` holds `v1.metadata.json` and `v2.metadata.json`, which carry the schema and the list of snapshots, two `snap-*.avro` manifest lists, two `*.avro` manifests listing the data files with their statistics, and `version-hint.text`, a one-byte file naming the current version. The data is in the same format in both cases: what Iceberg adds is the description.

A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?
=> On a plain directory, the reader listed the files at the start of the query and then reads them one by one. A concurrent overwrite deletes and recreates them, so the reader gets a `FileNotFoundException` or, worse, a mixture of old and new files: a result that corresponds to no consistent state of the table. With Iceberg the reader resolves `version-hint.text` once, pins a snapshot, and reads the file list of that snapshot. The writer creates new files and commits a new version; the old files are not touched until a maintenance operation expires them. The reader finishes on its snapshot and sees a coherent state.

With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?
=> The `hadoop` catalog commits by writing `version-hint.text`, which assumes the object store offers an atomic rename or a conditional write. On an S3-compatible store that guarantee is not general: two jobs can read version 2, each write a version 3, and the last write wins, so one commit is lost silently. A catalog service, Polaris, Nessie, Hive Metastore or Glue, holds the pointer in a transactional store and performs the swap as a compare-and-set: the loser receives an error and retries instead of overwriting. That is exactly what the next module introduces.

## Compare with the dbt lab

What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?
=> Measured with DuckDB on the two Parquet outputs: `order_id` and `user_id` are `UUID` on the dbt side and `VARCHAR` on the Spark side, and `ordered_at` is `TIMESTAMP WITH TIME ZONE` on the dbt side against a `TIMESTAMP` without zone on the Spark side. The identifiers were cast to string, as the lab asks. The consequences are concrete: 36 bytes instead of 16 per identifier, no validation that the value is a well-formed UUID, and a join between a dbt table and a Spark table requires an explicit cast on one side. The loss of the time zone is the more dangerous of the two, since an hour read as local time rather than UTC produces a wrong result without any error.

Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?
=> The single object is easier: one path, one file, readable by any tool without the notion of a directory-as-table. The directory of part-files scales, because the write and the read are parallel and because adding data does not mean rewriting a single object. The single object is also a dead end on volume, since one writer produces it. This is precisely the gap an open table format fills: a directory of files plus the metadata needed to treat it as one table.

The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?
=> Four `assert` statements on row counts, nothing more: they check that no row was lost in the Parquet and Iceberg round trips, not that the content is correct. Reaching the dbt level would mean writing the equivalent checks by hand, a `count` against a `countDistinct` for `unique`, a filter on `isNull` for `not_null`, an anti-join against the expected values for `accepted_values` and against the parent table for `relationships`, and a convention for what a failure does. In other words rebuilding a small test framework, which is what dbt provides declaratively. A library such as Great Expectations or Soda fills that role for Spark.

`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?
=> Nothing explicit. The order of the cells is the dependency graph, held in the author's head and enforced only by the fact that a Python variable must exist before it is used. There is no way to ask which models depend on `stg_orders`, no lineage, and no selective re-execution of a subgraph. Spark knows the lineage of a DataFrame, which is not the same thing: it describes one computation, not a catalogue of named models.

Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?
=> `dbt build --select silver` ran in about 12 to 15 s against 2 min for this script. The comparison means nothing. The Spark figure includes the resolution and the download of the Iceberg package from Maven Central, the start of a JVM, and the writing of Parquet and Iceberg outputs that dbt does not produce, since the dbt models are views. The dbt figure, for its part, includes 22 data tests that this lab does not run. On 324 KB of data, both measure their own startup cost, not their throughput.

For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?
=> 10 GB per day: dbt with DuckDB. That fits on one machine, and the JVM, the cluster and the shuffle are a cost with no counterpart. 10 TB per day: Spark, since the volume no longer fits on one node and the work must be distributed. A team of analysts who only know SQL: dbt, not for the engine but for the workflow, where a model is a `SELECT`, the dependencies are declared with `ref()`, and the tests are YAML. Spark SQL would let them write the queries but not manage a project.

## Exercises

1. `birthdate_is_valid` with the DataFrame API and with Spark SQL
=> Both versions join `stg_users` to the minimum `ordered_at` per user and flag the rows whose birthdate is later than that first order. They agree: 5 invalid birthdates out of 50 users, the same 5 found by the dbt model. The difference with the dbt lab is not the result but the placement. dbt puts the rule inside the `stg_users` model, so every consumer of the silver layer gets the NULL birthdate and the flag without knowing the rule exists. Here the rule lives in a cell of a notebook, applied to a DataFrame that nothing forces a consumer to use; the Parquet and Iceberg outputs written earlier do not carry it at all.

2. `zip_code` as a plain UDF and as a `pandas_udf`
=> Measured on `users_bronze`: `regexp_extract` 0.207 s, plain Python UDF 2.244 s, `pandas_udf` 1.289 s. The built-in expression is about eleven times faster than the Python UDF. The reason is where the code runs: `regexp_extract` is a Catalyst expression evaluated in the JVM on the internal row format, with no serialization. A Python UDF forces every row out of the JVM, into a Python process, and back, one row at a time. The `pandas_udf` keeps the Python process but sends batches through Arrow, which removes the per-row round trip and recovers part of the gap without closing it. The ordering is the point; the absolute figures are small because the dataset is 50 rows and dominated by fixed costs.

3. Cache the bronze DataFrames
=> Measured on `orders_bronze`: 0.187 s then 0.178 s without cache, 0.409 s then 0.121 s with it. The first cached call is the most expensive, since it computes and stores; the second is the fastest. The storage level is `Disk Memory Deserialized 1x Replicated`. As in the RDD lab, the gain is marginal here because the dataset is 324 KB and the read is cheap. The reason to cache a bronze DataFrame read with `inferSchema=True` is stronger than these figures suggest: without a cache, each action replays the inference pass over the file in addition to the read.

4. Append and time travel
=> One order was appended to `lab.silver.orders` with `writeTo(...).append()`: 2829 rows before, 2830 after. The table then exposes two snapshots, and `VERSION AS OF <first snapshot id>` still returns 2829 rows. The append created a new 1.9 KB data file next to the original 84 KB one, a new manifest and a `v2.metadata.json`, without touching anything written before. The Parquet directory offers nothing equivalent: appending means adding a part-file, and the only way back would be to know which files existed before, which no metadata records. An overwrite destroys the previous state outright. That is the difference between a set of files and a table format.
