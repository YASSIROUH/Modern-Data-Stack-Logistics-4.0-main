"""
Logistics 4.0 Streaming Data Processor.
Handles the ingestion of raw Kafka unstructured events into the Gold Datamart.
Integrates MLflow predictive ML logic synchronously on the streaming micro-batches.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.ml import PipelineModel

# ==============================================================================
# DATALAKE & DWH CONFIGURATION 
# ==============================================================================
DB_URL = "jdbc:postgresql://postgres:5432/logistics_db"
DB_PROPERTIES = {
    "user": "admin_logistics",
    "password": "securepassword123",
    "driver": "org.postgresql.Driver"
}
S3A_BRONZE_URL = "s3a://logistics-bronze/raw_orders"
MODEL_PATH = "/opt/logistics/spark_models/late_delivery_rf"

def define_bronze_schema() -> StructType:
    """Returns the strict enforcement schema for Kafka payload parsing."""
    return StructType([
        StructField("Order Id", IntegerType(), True),
        StructField("Customer Id", IntegerType(), True),
        StructField("Product Card Id", IntegerType(), True),
        StructField("order date (DateOrders)", TimestampType(), True),
        StructField("shipping date (DateOrders)", StringType(), True),
        StructField("Days for shipping (real)", IntegerType(), True),
        StructField("Days for shipment (scheduled)", IntegerType(), True),
        StructField("Benefit per order", DoubleType(), True),
        StructField("Sales per customer", DoubleType(), True),
        StructField("Order Item Total", DoubleType(), True),
        StructField("Order Item Discount", DoubleType(), True),
        StructField("Product Price", DoubleType(), True),
        StructField("Order Status", StringType(), True),
        StructField("Delivery Status", StringType(), True),
        StructField("Shipping Mode", StringType(), True),
        StructField("Late_delivery_risk", IntegerType(), True),
        StructField("Customer Country", StringType(), True),
        StructField("Customer City", StringType(), True),
        StructField("Order Country", StringType(), True),
        StructField("Order City", StringType(), True),
        StructField("Order Region", StringType(), True),
        StructField("Category Name", StringType(), True),
        StructField("Customer Segment", StringType(), True),
        StructField("Department Name", StringType(), True),
        StructField("Type", StringType(), True)
    ])

def load_to_silver_layer(batch_df: DataFrame, batch_id: int):
    """
    Spark Micro-Batch sink mapping inferred dataset into Postgres DWH tables.
    """
    silver_df = batch_df.select(
        col("`Order Id`").alias("order_id"),
        col("`Customer Id`").alias("customer_id"),
        col("`Product Card Id`").alias("product_card_id"),
        col("`Customer Segment`").alias("customer_segment"),
        col("`Customer Country`").alias("customer_country"),
        col("`Customer City`").alias("customer_city"),
        col("`Category Name`").alias("category_name"),
        col("`Department Name`").alias("department_name"),
        col("`Product Price`").alias("product_price"),
        col("`Order Country`").alias("order_country"),
        col("`Order Region`").alias("order_region"),
        col("`Order City`").alias("order_city"),
        col("`Type`").alias("type"),
        col("`Benefit per order`").alias("benefit_per_order"),
        col("`Sales per customer`").alias("sales_per_customer"),
        col("`Order Item Discount`").alias("order_item_discount"),
        col("`order date (DateOrders)`").alias("order_date"),
        col("`shipping date (DateOrders)`").cast(TimestampType()).alias("shipping_date"),
        col("`Days for shipping (real)`").alias("days_for_shipping_real"),
        col("`Days for shipment (scheduled)`").alias("days_for_shipment_scheduled"),
        col("`Order Item Total`").alias("order_item_total"),
        col("`Order Status`").alias("order_status"),
        col("`Delivery Status`").alias("delivery_status"),
        col("`Shipping Mode`").alias("shipping_mode"),
        col("is_delayed_actual"),
        col("is_delayed_prediction"),
        col("is_prediction_correct")
    )
    
    silver_df.write.jdbc(
        url=DB_URL, 
        table="silver_orders", 
        mode="append", 
        properties=DB_PROPERTIES
    )


class MedallionPipeline:
    """Core executor class for the Medallion Pipeline."""

    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Logistics40_Streaming_Medallion") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
            .config("spark.jars.ivy", "/tmp/.ivy") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def execute_streams(self):
        print("[INFO] Bootstrapping Medallion Streaming Architecture...")
        
        # 1. Source System: Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "dataco_orders") \
            .option("startingOffsets", "latest") \
            .load()

        # 2. Bronze Sink: Data Lake Dump
        df.selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .format("text") \
            .option("path", S3A_BRONZE_URL) \
            .option("checkpointLocation", "/opt/bitnami/spark/checkpoint_bronze") \
            .trigger(processingTime="10 seconds") \
            .start()

        # 3. Data Transformation & Validation
        schema = define_bronze_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        # 4. MLOps Inference Implementation
        try:
            print(f"[INFO] Initializing PySpark pipeline model from: {MODEL_PATH}")
            model = PipelineModel.load(MODEL_PATH)
            
            parsed_df_ml = parsed_df \
                .withColumn("Days for shipment (scheduled)", col("`Days for shipment (scheduled)`").cast("integer")) \
                .withColumn("Order Item Total", col("`Order Item Total`").cast("double")) \
                .withColumn("Shipping Mode", col("`Shipping Mode`")) \
                .withColumn("is_delayed_actual", col("Late_delivery_risk").cast("integer"))
                                    
            ml_df = model.transform(parsed_df_ml)
            
            enriched_df = ml_df \
                .withColumn("is_delayed_prediction", col("prediction").cast("integer")) \
                .withColumn("is_prediction_correct", (col("is_delayed_actual") == col("is_delayed_prediction")).cast("boolean"))
                               
        except Exception as e:
            print(f"[CRITICAL] ML Model load failure. Ensure execution of training protocol. Exception -> {e}")
            enriched_df = parsed_df \
                .withColumn("is_delayed_actual", col("Late_delivery_risk").cast("integer")) \
                .withColumn("is_delayed_prediction", lit(-1)) \
                .withColumn("is_prediction_correct", lit(False))

        # 5. Silver Sink: Database Flush
        enriched_df.writeStream \
            .foreachBatch(load_to_silver_layer) \
            .outputMode("append") \
            .option("checkpointLocation", "/opt/bitnami/spark/checkpoint_silver") \
            .trigger(processingTime="10 seconds") \
            .start()

        print("[INFO] Core Medallion layer streaming pipelines initialized successfully.")
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    pipeline = MedallionPipeline()
    pipeline.execute_streams()
