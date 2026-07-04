from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_values
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session with optimizations
spark = SparkSession.builder \
    .appName("RealtimeRecommender") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

logger.info("Spark Session initialized successfully")

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "click_events") \
    .option("maxOffsetsPerTrigger", "100000") \
    .option("failOnDataLoss", "false") \
    .load()

# Define schema for incoming data
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("event_type", StringType())
])

# Parse JSON data
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("user_id").isNotNull() & col("product_id").isNotNull())

# Add score based on event type using optimized when() chain
scored = parsed.withColumn(
    "score",
    when(col("event_type") == "page_view", 1)
    .when(col("event_type") == "add_to_cart", 5)
    .when(col("event_type") == "checkout", 7)
    .when(col("event_type") == "purchase", 10)
    .otherwise(1)  # Default score for unknown events
)

# Aggregate scores per user-product pair
agg = scored.groupBy("user_id", "product_id") \
    .agg(sum("score").alias("interaction_score"))

# PostgreSQL Batch Writer with Parallel Processing
class ParallelPostgresWriter:
    def __init__(self, num_partitions=10, batch_size=1000):
        self.num_partitions = num_partitions
        self.batch_size = batch_size
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "ecommerce_db",
            "user": "postgres",
            "password": "car.gemera",
            "connect_timeout": 10
        }
    
    def write_batch(self, batch_df, batch_id):
        """Write batch using parallel partition processing"""
        
        def write_partition(iterator):
            """Process each partition with its own connection"""
            conn = None
            cur = None
            partition_batch = []
            record_count = 0
            
            try:
                # Create connection per partition
                conn = psycopg2.connect(**self.db_config)
                conn.autocommit = False
                cur = conn.cursor()
                
                # Collect and batch process records from this partition
                for row in iterator:
                    partition_batch.append((row.user_id, row.product_id, row.interaction_score))
                    record_count += 1
                    
                    # When batch size is reached, execute
                    if len(partition_batch) >= self.batch_size:
                        self._execute_batch(cur, partition_batch)
                        partition_batch = []
                
                # Execute any remaining records
                if partition_batch:
                    self._execute_batch(cur, partition_batch)
                
                # Commit all changes for this partition
                conn.commit()
                
                if record_count > 0:
                    logger.info(f"Partition wrote {record_count} records")
                
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Error in partition write: {e}")
                raise e
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
        
        # Check if dataframe is not empty
        try:
            count = batch_df.count()
            if count > 0:
                # Repartition for parallelism
                batch_df.repartition(self.num_partitions).foreachPartition(write_partition)
                logger.info(f"Batch {batch_id}: Completed processing {count} records")
            else:
                logger.info(f"Batch {batch_id}: Empty batch, skipping")
        except Exception as e:
            logger.error(f"Error in batch {batch_id}: {e}")
            raise
    
    def _execute_batch(self, cur, batch):
        """Execute batch insert/update using execute_values"""
        try:
            execute_values(
                cur,
                """
                INSERT INTO user_recommendations (user_id, product_id, popularity_score)
                VALUES %s
                ON CONFLICT (user_id, product_id)
                DO UPDATE SET
                    popularity_score = user_recommendations.popularity_score + EXCLUDED.popularity_score
                """,
                batch,
                page_size=len(batch)
            )
        except Exception as e:
            logger.error(f"Error executing batch of size {len(batch)}: {e}")
            raise

# Create writer instance
writer = ParallelPostgresWriter(num_partitions=10, batch_size=1000)

# Create checkpoint directory with absolute path
checkpoint_dir = os.path.join(os.getcwd(), "checkpoints", "recommender")
os.makedirs(checkpoint_dir, exist_ok=True)
logger.info(f"Checkpoint directory: {checkpoint_dir}")

# Start the streaming query WITHOUT checkpoint first to test
query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(writer.write_batch) \
    .trigger(processingTime="5 seconds") \
    .start()

logger.info("Streaming query started successfully")

# Wait for termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Shutting down...")
    query.stop()
    spark.stop()
    logger.info("Application stopped")