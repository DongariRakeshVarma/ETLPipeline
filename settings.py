"""
Configuration settings for AWS ETL Pipeline.
Load secrets from environment variables — never hardcode credentials.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_CONFIG = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "region":     os.getenv("AWS_REGION", "us-east-1"),
}

# ── S3 ────────────────────────────────────────────────────────────────────────
S3_CONFIG = {
    "bucket":      os.getenv("S3_BUCKET", "your-bucket-name"),
    "input_path":  os.getenv("S3_INPUT_PATH",  "s3a://your-bucket/raw/data.csv"),
    "output_path": os.getenv("S3_OUTPUT_PATH", "s3a://your-bucket/processed/"),
}

# ── Redshift ──────────────────────────────────────────────────────────────────
REDSHIFT_CONFIG = {
    "host":         os.getenv("REDSHIFT_HOST", "your-cluster.redshift.amazonaws.com"),
    "port":         os.getenv("REDSHIFT_PORT", "5439"),
    "database":     os.getenv("REDSHIFT_DB",   "analytics"),
    "user":         os.getenv("REDSHIFT_USER",  "admin"),
    "password":     os.getenv("REDSHIFT_PASS",  ""),
    "target_table": os.getenv("REDSHIFT_TABLE", "public.etl_output"),
}

# ── Pipeline ──────────────────────────────────────────────────────────────────
PIPELINE_CONFIG = {
    "app_name":   "AWS-ETL-Pipeline",
    "batch_size": int(os.getenv("BATCH_SIZE", "10000")),
    "log_level":  os.getenv("LOG_LEVEL", "INFO"),
}
