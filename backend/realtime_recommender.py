from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2

spark = SparkSession.builder \
    .appName("RealtimeRecommender") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "click_events") \
    .load()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("event_type", StringType())
])

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

score_map = {
    "page_view": 1,
    "add_to_cart": 5,
    "checkout": 7,
    "purchase": 10
}

scored = parsed.withColumn(
    "score",
    when(col("event_type") == "page_view", 1)
    .when(col("event_type") == "add_to_cart", 5)
    .when(col("event_type") == "checkout", 7)
    .when(col("event_type") == "purchase", 10)
    .otherwise(1)
)

agg = scored.groupBy("user_id", "product_id") \
    .agg(sum("score").alias("interaction_score"))

def write_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce_db",
        user="postgres",
        password="car.gemera"
    )
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO user_recommendations (user_id, product_id, popularity_score, rank)
            VALUES (%s, %s, %s, 999)
            ON CONFLICT (user_id, product_id)
            DO UPDATE SET
                popularity_score = user_recommendations.popularity_score + EXCLUDED.popularity_score
        """, (row.user_id, row.product_id, row.interaction_score))
    conn.commit()
    cur.close()
    conn.close()

query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()