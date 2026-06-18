import os
import uuid
from datetime import datetime
from openai import OpenAI
from prefect import task, flow, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from dotenv import load_dotenv

# Force environment stability profiles
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PREFECT_API_ENABLE_ANALYTICS"] = "false"

load_dotenv()

# =====================================================================
# Production-Grade Configuration
# =====================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "knowledge-factory")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

#  SILICONFLOW EMBEDDING API GATEWAY CONFIG
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY")

API_BASE_URL = os.getenv("EMBEDDING_API_BASE", "https://api.siliconflow.cn/v1")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-zh-v1.5")

# single embeddings.create request can send up to 32 chunks at a time
# adjust as needed based on your token limits and latency requirements
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError("Critical storage credentials missing! Check your .env file.")

if not SILICONFLOW_API_KEY:
    raise ValueError("SILICONFLOW_API_KEY is missing in your .env configuration!")

def get_spark_session():
    # read our local jars once to avoid repeated downloads
    jar_path = "./jars/*"
    return SparkSession.builder \
        .appName("KnowledgeFactory-GoldEmbedding") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

@task
def fetch_unprocessed_silver_papers() -> list:
    logger = get_run_logger()
    spark = get_spark_session()

    silver_table_path = f"s3a://{BUCKET_NAME}/silver/fact_arxiv_papers"
    logger.info("Scanning Silver layer for data assets...")

    try:
        df = spark.read.format("delta").load(silver_table_path)
        records = df.select("paper_id", "title", "full_text").collect()

        logger.info(f"Successfully retrieved {len(records)} papers from Silver layer.")
        return [{
            "arxiv_id": r["paper_id"],
            "title": r["title"],
            "full_text": r["full_text"]
        } for r in records]
    except Exception as e:
        logger.warning(f"Silver layer table not ready or empty: {str(e)}")
        return []

@task
def chunk_text_sliding_window(text: str, chunk_size: int = 200, overlap: int = 40) -> list:
    logger = get_run_logger()
    if not text:
        return []

    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
        if i + chunk_size >= len(words):
            break

    logger.info(f"Text tokenized into {len(chunks)} historical semantic blocks.")
    return chunks

def _chunked(items: list, size: int):
    """Chunk a list into multiple sublists of a fixed size"""
    for i in range(0, len(items), size):
        yield i, items[i:i + size]

@task(retries=3, retry_delay_seconds=15)
def generate_embeddings_via_api(paper_id: str, title: str, chunks: list):
    logger = get_run_logger()
    spark = get_spark_session()

    if not chunks:
        return

    logger.info(f"🚀 Batching active: Offloading {len(chunks)} chunks to SiliconFlow embedding endpoint...")

    client = OpenAI(api_key=SILICONFLOW_API_KEY, base_url=API_BASE_URL)

    schema = StructType([
        StructField("chunk_id", StringType(), False),
        StructField("paper_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("chunk_index", IntegerType(), False),
        StructField("chunk_text", StringType(), True),
        StructField("vector_string", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    gold_records = []

    try:
        # Iterate through chunks in batches to respect API limits and optimize throughput
        for offset, batch in _chunked(chunks, EMBEDDING_BATCH_SIZE):
            response = client.embeddings.create(
                input=batch,
                model=EMBEDDING_MODEL,
                timeout=45.0
            )

            for local_idx, item in enumerate(response.data):
                global_idx = offset + local_idx
                embedding = item.embedding
                vector_str = ",".join(map(str, embedding))
                unique_chunk_id = f"chk_{uuid.uuid4().hex[:12]}"

                gold_records.append((
                    unique_chunk_id,
                    paper_id,
                    title,
                    global_idx,
                    chunks[global_idx],
                    vector_str,
                    datetime.now()
                ))

        df = spark.createDataFrame(gold_records, schema)
        gold_table_path = f"s3a://{BUCKET_NAME}/gold/fact_paper_embeddings"

        logger.info(f"✅ Appended {len(gold_records)} high-dimensional vector nodes to Gold Layer.")
        df.write.format("delta").mode("append").save(gold_table_path)

    except Exception as e:
        logger.error(f"Flight collision on paper {paper_id}: {str(e)}")
        raise e

@flow(name="ArXiv-Gold-Embedding-Pipeline")
def silver_to_gold_flow():
    logger = get_run_logger()
    silver_papers = fetch_unprocessed_silver_papers()

    if not silver_papers:
        logger.warning("No fresh data assets available. Pipeline idling.")
        return

    for paper in silver_papers:
        text_chunks = chunk_text_sliding_window(text=paper["full_text"])
        generate_embeddings_via_api(
            paper_id=paper["arxiv_id"],
            title=paper["title"],
            chunks=text_chunks
        )

    spark = get_spark_session()
    gold_table_path = f"s3a://{BUCKET_NAME}/gold/fact_paper_embeddings"
    logger.info("--- Inspecting Final Delta Gold Vector Embeddings Table ---")
    try:
        spark.read.format("delta").load(gold_table_path) \
            .select("chunk_id", "paper_id", "chunk_index", "vector_string") \
            .show(3, truncate=25)
    except Exception as e:
        logger.error(f"Snapshot inspection failed: {str(e)}")

if __name__ == "__main__":
    silver_to_gold_flow()