import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_session():
    """Create a SparkSession configured for Apache Spark 3.5.0, Delta Lake, and MinIO."""
    # Explicitly pull the matched version of Delta and Hadoop AWS connectors
    SUBMIT_ARGS = (
        "--packages io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4 "
        "pyspark-shell"
    )
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

    builder = (
        SparkSession.builder
        .appName("KnowledgeFactory-ConnectivityCheck")
        # Connect to your freshly booted official spark master container
        .master("spark://localhost:7077")
        .config("spark.driver.host", "10.0.0.193")     
    .config("spark.driver.bindAddress", "0.0.0.0") 
        # Inject Delta Lake Extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO S3A Configurations
        #.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://10.0.0.193:9000") 
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Fix potential local directory permission lockouts during shuffling
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp")
    )
    return builder.getOrCreate()

def main():
    print("Initializing Spark Session via Official Master...")
    spark = create_spark_session()
    
    try:
        # Mock schema for checking ArXiv processing pipeline health
        schema = StructType([
            StructField("paper_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        data = [
            ("2605.0001", "Attention Is All You Need", "Verified"),
            ("2605.0002", "Medallion Design in RAG Systems", "Verified")
        ]
        
        print("Generating Mock DataFrame...")
        df = spark.createDataFrame(data, schema)
        
        # Pointing to the bucket automatically initialized by your 'minio-create-buckets' service
        target_path = "s3a://lakehouse/bronze/connectivity_test"
        
        print(f"Committing Delta transaction to: {target_path}...")
        df.write.format("delta").mode("overwrite").save(target_path)
        print("Delta write transaction logged successfully!")
        
        print("Reading validation track back from MinIO...")
        result_df = spark.read.format("delta").load(target_path)
        result_df.show()
        print(" [SUCCESS] Local factory engine is fully operational!")
        
    except Exception as e:
        print(f"[FAILURE] Pipeline test blocked! Error Stack:\n{str(e)}")
        sys.exit(1)
    finally:
        print("🔌 Disconnecting Spark Session...")
        spark.stop()

if __name__ == "__main__":
    main()