import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, when, count, sum, avg

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

# Read from silver tables
logger.info("Reading data from silver tables...")

flights_df = spark.read.table(f"s3tablesbucket.silver.us_flights")
cancelled_df = spark.read.table(f"s3tablesbucket.silver.cancelled_diverted")

logger.info("Data read successfully. Starting flight performance analysis...")

# Create comprehensive flight performance analytics
flight_performance = flights_df.groupBy(
    "airline",
    year("flight_date").alias("year"),
    month("flight_date").alias("month")
).agg(
    count("*").alias("total_flights"),
    avg("dep_delay_min").alias("avg_departure_delay_min"),
    avg("arr_delay_min").alias("avg_arrival_delay_min"),
    avg("flight_duration_min").alias("avg_flight_duration_min"),
    sum(when(col("dep_delay_tag") == True, 1).otherwise(0)).alias("delayed_flights"),
    avg("delay_carrier_min").alias("avg_carrier_delay"),
    avg("delay_weather_min").alias("avg_weather_delay"),
    avg("delay_nas_min").alias("avg_nas_delay"),
    avg("delay_security_min").alias("avg_security_delay")
).withColumn(
    "on_time_percentage", 
    ((col("total_flights") - col("delayed_flights")) / col("total_flights") * 100)
)

logger.info("Flight performance analysis completed. Writing to gold table...")
# Write to gold table
try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}")
    spark.sql(f"DROP TABLE IF EXISTS s3tablesbucket.{namespace}.flight_performance_analytics")
    flight_performance.writeTo(f"s3tablesbucket.{namespace}.flight_performance_analytics") \
      .using("Iceberg") \
      .tableProperty("format-version", "2") \
      .createOrReplace()
except Exception as e:
    logger.error(f"Error writing to gold table: {e}")
    raise e

logger.info("Data written to gold table successfully.")

job.commit()