import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import boto3
from botocore.client import Config
from prefect import task, flow, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from dotenv import load_dotenv

# Load local environment variables from .env file immediately upon startup
load_dotenv()

# =====================================================================
# Production-Grade Configuration
# =====================================================================
# Read environment variables first, fallback to local default values if not found
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "knowledge-factory")


# Fetch credentials securely from system environment variables loaded via .env
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Defensive Assertion: Prevent obscure connection failures caused by missing configs
if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError(
        "Critical credentials missing! Please ensure that a .env file exists "
        "in the project root directory containing valid AWS_ACCESS_KEY_ID and "
        "AWS_SECRET_ACCESS_KEY configurations."
    )

def get_spark_session():
    """
    Initializes and returns a Spark session configured for Delta Lake and MinIO (S3A).
    """
    return SparkSession.builder \
        .appName("KnowledgeFactory-BronzeIngestion") \
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
#  Dynamic Discovery via arXiv API
# =====================================================================
@task(retries=3, retry_delay_seconds=10)
def fetch_arxiv_paper_list(keyword: str, max_results: int = 3) -> list:
    """
    Queries the official arXiv API dynamically based on a search keyword.
    Parses the Atom XML response and extracts available paper metadata.
    """
    logger = get_run_logger()
    logger.info(f"Querying arXiv API for keyword: '{keyword}' (Max results: {max_results})")
    
    # URL encode the keyword to handle spaces safely
    encoded_keyword = requests.utils.quote(f'all:"{keyword}"')
    api_url = f"http://export.arxiv.org/api/query?search_query={encoded_keyword}&max_results={max_results}"
    
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"arXiv API returned unexpected status code: {response.status_code}")
        
    # Parse XML response
    root = ET.fromstring(response.content)
    papers = []
    
    # Atom XML namespaces mapping
    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    
    for entry in root.findall('atom:entry', ns):
        # Extract metadata fields safely
        raw_id_url = entry.find('atom:id', ns).text
        arxiv_id = raw_id_url.split('/abs/')[-1].split('v')[0] # Standardize ID format
        
        title = entry.find('atom:title', ns).text.strip().replace('\n', ' ')
        
        # Locate the specific direct PDF link among multiple link tags
        download_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
        
        papers.append({
            "arxiv_id": arxiv_id,
            "title": title,
            "download_url": download_url
        })
        
    logger.info(f"Successfully discovered {len(papers)} papers for keyword: '{keyword}'")
    return papers
@task(retries=3, retry_delay_seconds=10)
def download_and_upload_to_bronze(arxiv_id: str, download_url: str) -> str:
    """
    Downloads a PDF from a remote URL and streams it directly to MinIO Bronze layer.
    This bypasses local disk caching to achieve a stateless and low-memory transfer.
    """
    logger = get_run_logger()
    logger.info(f"Starting Bronze ingestion for paper ID: {arxiv_id}")
    
    # Initialize S3 client using dynamic, secure credentials
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    
    # Construct S3 path following Medallion architecture and Hive partitioning standards
    today_str = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"bronze/arxiv_papers/ingestion_date={today_str}/{arxiv_id}.pdf"
    
    # Stream download from remote to minimize memory footprint
    response = requests.get(download_url, stream=True)
    if response.status_code == 200:
        # Stream raw bytes directly to the targeted storage bucket
        s3_client.upload_fileobj(response.raw, BUCKET_NAME, s3_key)
        logger.info(f"Successfully uploaded {arxiv_id}.pdf to bucket: {BUCKET_NAME}, path: {s3_key}")
        return s3_key
    else:
        raise Exception(f"Failed to download PDF from {download_url}, status code: {response.status_code}")

@task
def log_processing_event_to_delta(arxiv_id: str, s3_path: str, status: str):
    """
    Logs the metadata of each ingestion transaction into a Delta Lake table
    for auditing, observability, and data lineage tracing purposes.
    """
    logger = get_run_logger()
    spark = get_spark_session()
    
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("paper_id", StringType(), True),
        StructField("storage_path", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    event_id = f"evt_{int(datetime.now().timestamp())}"
    event_data = [(event_id, arxiv_id, s3_path, status, datetime.now())]
    
    df = spark.createDataFrame(event_data, schema)
    target_table_path = f"s3a://{BUCKET_NAME}/metadata/fact_processing_events"
    
    logger.info(f"Appending audit log event {event_id} into the Delta log...")
    df.write.format("delta").mode("append").save(target_table_path)
    
    print("--- Delta Processing Events Fact Table Status ---")
    spark.read.format("delta").load(target_table_path).show(truncate=False)

@flow(name="ArXiv-Bronze-Ingestion-Pipeline")
def arxiv_to_bronze_flow(search_keyword: str = "LLM observability", count: int = 3):
    """
    Dynamic workflow that searches, discovers, and ingests batch records seamlessly.
    """
    # 1. Dynamically discover what needs to be downloaded today
    discovered_papers = fetch_arxiv_paper_list(keyword=search_keyword, max_results=count)
    
    # 2. Iterate through the lineage pipeline dynamically
    for paper in discovered_papers:
        try:
            s3_path = download_and_upload_to_bronze(
                arxiv_id=paper["arxiv_id"], 
                download_url=paper["download_url"]
            )
            log_processing_event_to_delta(paper["arxiv_id"], s3_path, "INGESTED")
        except Exception as e:
            log_processing_event_to_delta(paper["arxiv_id"], "", f"FAILED: {str(e)}")
if __name__ == "__main__":
    # Feel free to change this keyword to anything you love! (e.g., "Vector database", "Agentic")
    arxiv_to_bronze_flow(search_keyword="LLM observability", count=3)