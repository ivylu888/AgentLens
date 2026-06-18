import os
import io
from datetime import datetime
import boto3
from botocore.client import Config
from pypdf import PdfReader
from prefect import task, flow, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# Production-Grade Configuration
# =====================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "knowledge-factory")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError("Critical credentials missing! Check your .env file.")

def get_spark_session():
    return SparkSession.builder \
        .appName("KnowledgeFactory-SilverProcessing") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# =====================================================================
# ✨ NEW TASK: Smart Audit Log Discovery (自動巡邏)
# =====================================================================
@task
def fetch_today_ingested_papers() -> list:
    """
    Queries the Bronze Delta audit log table to dynamically find all papers 
    successfully ingested on the current date. Eliminates hardcoding.
    """
    logger = get_run_logger()
    spark = get_spark_session()
    
    audit_table_path = f"s3a://{BUCKET_NAME}/metadata/fact_processing_events"
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Scanning Bronze audit log for target date: {today_str}")
    
    try:
        # Load the Delta table and filter for today's successfully ingested rows
        df = spark.read.format("delta").load(audit_table_path)
        
        # Filter logic: date matches today AND status is INGESTED
        today_papers = df.filter(
            (df.status == "INGESTED") & 
            (df.storage_path.contains(f"ingestion_date={today_str}"))
        ).select("paper_id", "storage_path").collect()
        
        # Convert Spark Rows into a clean Python list of dictionaries
        paper_list = [{"arxiv_id": row["paper_id"], "s3_path": row["storage_path"]} for row in today_papers]
        logger.info(f"Audit discovery complete. Found {len(paper_list)} targets ready for Silver processing.")
        return paper_list
        
    except Exception as e:
        logger.warning(f"Could not read audit log (it might be empty or missing): {str(e)}")
        return []

@task(retries=2, retry_delay_seconds=5)
def extract_text_from_bronze_pdf(s3_path: str) -> str:
    logger = get_run_logger()
    logger.info(f"Downloading raw PDF stream from Bronze path: {s3_path}")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_path)
    pdf_bytes = response['Body'].read()
    
    pdf_file = io.BytesIO(pdf_bytes)
    reader = PdfReader(pdf_file)
    
    full_text = ""
    for page in reader.pages:
        text = page.extract_text()
        if text:
            full_text += text + "\n"
            
    return full_text.strip()

@task
def save_cleaned_to_silver_delta(arxiv_id: str, full_text: str):
    logger = get_run_logger()
    spark = get_spark_session()
    
    schema = StructType([
        StructField("paper_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("full_text", StringType(), True),
        StructField("processed_timestamp", TimestampType(), True)
    ])
    
    # Simple extraction heuristic for fallback metadata
    title_placeholder = "Dynamic Processed Paper"
    abstract_placeholder = full_text[:200] + "..." if len(full_text) > 200 else ""
        
    cleaned_data = [(arxiv_id, title_placeholder, abstract_placeholder, full_text, datetime.now())]
    df = spark.createDataFrame(cleaned_data, schema)
    
    target_table_path = f"s3a://{BUCKET_NAME}/silver/fact_arxiv_papers"
    
    logger.info(f"Appending structured schema for paper {arxiv_id} into Silver Delta Lake...")
    df.write.format("delta").mode("append").save(target_table_path)

# =====================================================================
# FULLY DECOUPLED DYNAMIC FLOW
# =====================================================================
@flow(name="ArXiv-Silver-Processing-Pipeline")
def bronze_to_silver_flow():
    """
    Main Prefect flow that drives dynamic Silver layer processing using metadata logs.
    """
    logger = get_run_logger()
    
    # 1. Automatically discover what was ingested today via Spark SQL / Delta
    targets = fetch_today_ingested_papers()
    
    if not targets:
        logger.info("No newly ingested Bronze papers found for processing today.")
        return
        
    # 2. Loop through discovered targets dynamically without hardcoded inputs
    for paper in targets:
        try:
            logger.info(f"Processing target lineage chain for: {paper['arxiv_id']}")
            extracted_text = extract_text_from_bronze_pdf(paper["s3_path"])
            
            if not extracted_text:
                logger.warning(f"Empty text content extracted for: {paper['arxiv_id']}")
                continue
                
            save_cleaned_to_silver_delta(arxiv_id=paper["arxiv_id"], full_text=extracted_text)
            
        except Exception as e:
            logger.error(f"Failed to move paper {paper['arxiv_id']} to Silver: {str(e)}")

    # 3. Print the final state of our data lake asset for verification
    spark = get_spark_session()
    target_table_path = f"s3a://{BUCKET_NAME}/silver/fact_arxiv_papers"
    print("--- Final Delta Silver Papers Fact Table Log ---")
    spark.read.format("delta").load(target_table_path).select("paper_id", "processed_timestamp").show(truncate=False)

if __name__ == "__main__":
    bronze_to_silver_flow()