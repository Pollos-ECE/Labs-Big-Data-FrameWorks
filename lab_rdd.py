"""Module 6 - Introduction to Spark's RDD API.

The lab is written for the interactive pyspark shell. It is reproduced here as a
single script so every result is reproducible: spark-submit --master 'local[*]' lab_rdd.py
"""

import re
import time
from operator import add

from pyspark.sql import SparkSession


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

section("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users partitions :", users_lines.getNumPartitions())
print("orders partitions:", orders_lines.getNumPartitions())
print("after repartition(8):", orders_lines.repartition(8).getNumPartitions())
print("default parallelism:", sc.defaultParallelism)

section("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
start = time.perf_counter()
print("count 1:", orders_data.count(), f"({time.perf_counter() - start:.3f}s)")
start = time.perf_counter()
print("count 2:", orders_data.count(), f"({time.perf_counter() - start:.3f}s)")

section("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(repr(line))
print("raw line count:", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("record count  :", users_records.count())

section("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
for order in orders_rdd.take(3):
    print(order)

section("Key-value RDDs: total quantity per user")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey:", user_quantity.take(3))
user_quantity_grouped = (
    orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
)
print("groupByKey :", user_quantity_grouped.take(3))
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

section("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("before:", orders_rdd.count(), "after:", orders_dedup.count())

section("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for user_id, (order, username) in enriched.take(2):
    print(user_id, "->", username, "|", order["product"], order["quantity"])
print("joined rows:", enriched.count())

section("Exercise 1 - aggregateByKey")
# (order_count, total_quantity) per user in a single pass
per_user = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, qty: (acc[0] + 1, acc[1] + qty),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
average_quantity = per_user.mapValues(lambda v: v[1] / v[0])
for user_id, (count, total) in per_user.take(3):
    print(f"{user_id} orders={count} quantity={total}")
print("average per user:", [(u, round(a, 2)) for u, a in average_quantity.take(3)])
print("consistent with reduceByKey:", sorted(per_user.mapValues(lambda v: v[1]).collect()) == sorted(user_quantity.collect()))

section("Exercise 2 - leftOuterJoin with an orphan order")
FAKE_USER = "00000000-0000-0000-0000-000000000000"
fake_order = dict(orders_rdd.first())
fake_order["user_id"] = FAKE_USER
orders_with_orphan = orders_kv.union(sc.parallelize([(FAKE_USER, fake_order)]))
left_joined = orders_with_orphan.leftOuterJoin(users_kv)
orphans = left_joined.filter(lambda kv: kv[1][1] is None)
print("inner join rows     :", orders_with_orphan.join(users_kv).count())
print("left outer join rows:", left_joined.count())
print("orphan orders       :", orphans.count())
print("orphan sample       :", orphans.map(lambda kv: (kv[0], kv[1][1])).take(1))

section("Exercise 3 - the same aggregation with the DataFrame API")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
quantity_df.show(3, truncate=False)
print("--- DataFrame explain ---")
quantity_df.explain()
print("--- RDD toDebugString ---")
print(user_quantity.toDebugString().decode())

section("Exercise 4 - the effect of cache()")


def timed(label, rdd):
    start = time.perf_counter()
    value = rdd.count()
    first = time.perf_counter() - start
    start = time.perf_counter()
    rdd.count()
    second = time.perf_counter() - start
    print(f"{label}: count={value} first={first:.3f}s second={second:.3f}s")


uncached = orders_data.map(parse_order)
timed("uncached parse ", uncached)
timed("uncached agg   ", uncached.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add))
cached = orders_data.map(parse_order).cache()
timed("cached parse   ", cached)
timed("cached agg     ", cached.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add))
print("storage levels:", "uncached", uncached.getStorageLevel(), "| cached", cached.getStorageLevel())

spark.stop()
