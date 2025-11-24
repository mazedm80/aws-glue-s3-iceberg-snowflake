import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, trim, upper
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# Get job parameters
args = getResolvedOptions(sys.argv,['JOB_NAME', 'input_bucket', 'output_bucket_arn', 'namespace', 'env'])
input_bucket = args['input_bucket']
output_bucket_arn = args['output_bucket_arn']
namespace = args['namespace']
env = args['env']

# Set Spark configuration for Iceberg
def setSparkIcebergConf(env: str) -> SparkConf:
  conf_list = [
      ("spark.sql.catalog.s3tablesbucket","org.apache.iceberg.spark.SparkCatalog"),
      ("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog"),
      ("spark.sql.catalog.s3tablesbucket.warehouse", output_bucket_arn),
      ("spark.sql.defaultCatalog", "s3tablesbucket"),
      ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    ]
  if env == "dev":
    conf_list.append(("spark.jars", "/home/hadoop/workspace/src/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"))
  spark_conf = SparkConf().setAll(conf_list)
  return spark_conf

# Initialize Glue context and Spark session
conf = setSparkIcebergConf(env)
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schema for the CSV data
schema = StructType([
    StructField("airport_id", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
])

logger.info(f"Reading CSV data from: {input_bucket}")

try:
  df_original = spark.read.csv(f"s3://{input_bucket}/data/airports_geolocation.csv", header=True, schema=schema)
except Exception as e:
  logger.error(f"Error reading CSV data: {e}")
  raise e

logger.info("CSV data read successfully. Starting transformation...")

df_transformed = df_original.withColumn("airport_id", upper(trim(col("airport_id")))) \
    .withColumn("airport", trim(col("airport"))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state", upper(trim(col("state")))) \
    .withColumn("country", upper(trim(col("country")))) \
    .withColumn("processed_timestamp", current_timestamp())

df_clean = df_transformed.dropna(subset=["airport_id"])
df_final = df_clean.dropDuplicates(["airport_id"])
df_sorted = df_final.orderBy("airport_id")

logger.info("Data transformation complete. Writing to Iceberg table...")

try:
  spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}")
  
  df_sorted.writeTo(f"s3tablesbucket.{namespace}.airport_geolocation") \
    .using("Iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
except Exception as e:
  logger.error(f"Error writing to Iceberg table: {e}")
  raise e

logger.info("Data written to Iceberg table successfully.")

job.commit()