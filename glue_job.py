"""
AWS Glue Job — Serverless ETL
Runs the same pipeline logic natively on AWS Glue infrastructure.
Deploy this script directly to your Glue job in the AWS Console.
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, trim, upper, current_timestamp, lit, when

# ── Init ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT", "S3_OUTPUT", "REDSHIFT_TABLE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
logger.info(f"Input:  {args['S3_INPUT']}")
logger.info(f"Output: {args['S3_OUTPUT']}")

# ── Extract ───────────────────────────────────────────────────────────────────
logger.info("Extracting data from S3...")
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [args["S3_INPUT"]], "recurse": True},
    transformation_ctx="datasource"
)
df = datasource.toDF()
logger.info(f"Records extracted: {df.count()}")

# ── Transform ─────────────────────────────────────────────────────────────────
logger.info("Applying transformations...")
df = df.dropna(subset=["id", "date", "amount"])

string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
for c in string_cols:
    df = df.withColumn(c, trim(upper(col(c))))

df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
df = df.withColumn("amount", when(col("amount").cast("double") < 0, lit(0.0)).otherwise(col("amount").cast("double")))
df = df.withColumn("etl_loaded_at", current_timestamp())
df = df.withColumn("etl_job", lit(args["JOB_NAME"]))

logger.info(f"Records after transformation: {df.count()}")

# ── Load to S3 (Parquet) ──────────────────────────────────────────────────────
logger.info(f"Writing output to S3: {args['S3_OUTPUT']}")
df.write.mode("overwrite").parquet(args["S3_OUTPUT"])

# ── Load to Redshift ──────────────────────────────────────────────────────────
logger.info(f"Loading into Redshift: {args['REDSHIFT_TABLE']}")
from awsglue.dynamicframe import DynamicFrame
dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_df,
    catalog_connection="redshift-connection",
    connection_options={"dbtable": args["REDSHIFT_TABLE"], "database": "analytics"},
    redshift_tmp_dir=args["S3_OUTPUT"] + "tmp/",
    transformation_ctx="redshift_output"
)

logger.info("Glue Job completed successfully ✅")
job.commit()
