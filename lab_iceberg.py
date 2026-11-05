# Module 8 - Explore Iceberg tables with Spark.
# The silver DataFrames of module 7 are read back from the Parquet outputs of that
# lab, so this script does not depend on a live session from the previous lab.

import os

from pyspark.sql import SparkSession, functions as F


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

section("Create an Iceberg table")
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
stg_users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

section("Explore Iceberg metadata")
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
print("snapshots :", spark.sql("SELECT * FROM lab.silver.orders.snapshots").count())
print("data files:", spark.sql("SELECT * FROM lab.silver.orders.files").count())

section("Create a second snapshot")
new_orders = spark.table("lab.silver.orders").limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
spark.sql("SELECT count(*) AS rows FROM lab.silver.orders").show()
print("data files after append:", spark.sql("SELECT * FROM lab.silver.orders.files").count())

section("Time travel")
snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
print("first snapshot:", first_snapshot)
spark.sql(f"SELECT count(*) AS rows_at_first_snapshot FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
spark.sql("SELECT count(*) AS rows_now FROM lab.silver.orders").show()

section("Schema evolution")
files_before = {r["file_path"] for r in spark.sql("SELECT file_path FROM lab.silver.orders.files").collect()}
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("INSERT INTO lab.silver.orders SELECT order_id, user_id, ordered_at, quantity, product, 'spark' FROM lab.silver.orders LIMIT 1")
files_after = {r["file_path"] for r in spark.sql("SELECT file_path FROM lab.silver.orders.files").collect()}
print("files kept untouched:", len(files_before & files_after), "of", len(files_before))
print("files added         :", len(files_after - files_before))
spark.sql("SELECT source, count(*) AS rows FROM lab.silver.orders GROUP BY source").show()

section("Exercises 1 to 5 on lab.silver.users")
stg_users.writeTo("lab.silver.users").createOrReplace()
before = spark.table("lab.silver.users").count()
print("users after create:", before)

new_users = (
    spark.table("lab.silver.users").limit(5)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat(F.col("username"), F.lit("_batch2")))
    .withColumn("email", F.concat(F.lit("batch2_"), F.col("email")))
)
new_users.writeTo("lab.silver.users").append()
after = spark.table("lab.silver.users").count()
print("users after append:", after)
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.users.snapshots ORDER BY committed_at").show(truncate=False)

user_snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()
first_user_snapshot = user_snapshots[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS rows_before FROM lab.silver.users VERSION AS OF {first_user_snapshot}").show()
spark.sql("SELECT count(*) AS rows_now FROM lab.silver.users").show()

spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql(
    "INSERT INTO lab.silver.users SELECT user_id, username, name, sex, email, birthdate,"
    " street, city, state, zip_code, 'US' FROM lab.silver.users LIMIT 2"
)
spark.sql("SELECT country, count(*) AS rows FROM lab.silver.users GROUP BY country").show()
spark.sql("DESCRIBE lab.silver.users").show()

print("--- layout comparison ---")
for table in ("users", "orders"):
    files = spark.sql(f"SELECT count(*) AS n, sum(record_count) AS rows, sum(file_size_in_bytes) AS bytes FROM lab.silver.{table}.files").collect()[0]
    snaps = spark.sql(f"SELECT count(*) AS n FROM lab.silver.{table}.snapshots").collect()[0]["n"]
    print(f"{table:7s} data_files={files['n']} rows={files['rows']} bytes={files['bytes']} snapshots={snaps}")

spark.stop()
