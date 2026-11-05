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
