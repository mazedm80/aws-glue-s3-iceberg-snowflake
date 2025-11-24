import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, upper, trim
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType, StringType, StructField, StructType

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
    StructField("flight_date", DateType(), True),
    StructField("day_of_week", IntegerType(), True),  # TINYINT
    StructField("airline", StringType(), True),
    StructField("tail_number", StringType(), True),
    StructField("dep_airport", StringType(), True),
    StructField("dep_city", StringType(), True),
    StructField("dep_time_label", StringType(), True),
    StructField("dep_delay_min", FloatType(), True),
    StructField("dep_delay_tag", IntegerType(), True),  # Will convert to Boolean later
    StructField("dep_delay_type", StringType(), True),
    StructField("arr_airport", StringType(), True),
    StructField("arr_city", StringType(), True),
    StructField("arr_delay_min", FloatType(), True),
    StructField("arr_delay_type", StringType(), True),
    StructField("flight_duration_min", FloatType(), True),
    StructField("distance_type", StringType(), True),
    StructField("delay_carrier_min", FloatType(), True),
    StructField("delay_weather_min", FloatType(), True),
    StructField("delay_nas_min", FloatType(), True),
    StructField("delay_security_min", FloatType(), True),
    StructField("delay_last_aircraft_min", FloatType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("aircraft_age", IntegerType(), True),
])

logger.info(f"Reading CSV data from: {input_bucket}")

try:
  df_original = spark.read.csv(f"s3://{input_bucket}/US_flights_2023.csv", header=True, schema=schema)
except Exception as e:
  logger.error(f"Error reading CSV data: {e}")
  raise e

logger.info("CSV data read successfully. Starting transformation...")

df_transformed = df_original.withColumn("dep_delay_tag", col("dep_delay_tag").cast(BooleanType())) \
    .withColumn("airline", trim(col("airline"))) \
    .withColumn("tail_number", upper(trim(col("tail_number")))) \
    .withColumn("dep_airport", upper(trim(col("dep_airport")))) \
    .withColumn("arr_airport", upper(trim(col("arr_airport")))) \
    .withColumn("processed_timestamp", current_timestamp())

df_clean = df_transformed.dropna(subset=["flight_date", "airline", "dep_airport", "arr_airport"])
df_final = df_clean.dropDuplicates(["flight_date", "airline", "tail_number", "dep_airport", "arr_airport"])
df_sorted = df_final.orderBy("flight_date", "airline", "dep_airport")

logger.info("Data transformation complete. Writing to Iceberg table...")

try:
  spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}")

  df_sorted.writeTo(f"s3tablesbucket.{namespace}.us_flights") \
    .using("Iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
except Exception as e:
  logger.error(f"Error writing to Iceberg table: {e}")
  raise e

logger.info("Data written to Iceberg table successfully.")

job.commit()