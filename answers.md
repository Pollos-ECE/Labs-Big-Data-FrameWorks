Why are the credentials stored in a Secret and not in the ConfigMap?
=> for them not to be leaked anywhere, a configmap is for non sensitive informations and is clear text that anyone that can access the configuration could read it , sensitive informations needs to be crypted 

The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?
=> If the job is executed again tomoroww it would fail. Once the secret has expired, the s3 backend rejects it.
A production platform would provide credentials to a job by rotating automaticaly the credentials and keeping the kubernetes secret updated

How would you turn this Job into a daily ingestion?
=> By scheduling a run everyday at the same time and being carefull to overlapping runs


The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?


The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?

The dataset contains 50 users, each one with at least one order, and 2829 orders with unique identifiers. Why does approx_unique return 40 and 3167?

The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?


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