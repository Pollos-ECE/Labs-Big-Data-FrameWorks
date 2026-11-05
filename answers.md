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