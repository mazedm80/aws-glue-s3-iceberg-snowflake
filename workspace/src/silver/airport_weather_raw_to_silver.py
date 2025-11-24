import sys
from awsglue.utils import getResolvedOptions
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, trim, upper
from pyspark.sql.types import DateType, FloatType, StringType, StructField, StructType

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
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schema for the CSV data
schema = StructType([
    StructField("weather_date", DateType(), True),
    StructField("tavg_c", FloatType(), True),
    StructField("tmin_c", FloatType(), True),
    StructField("tmax_c", FloatType(), True),
    StructField("precip_mm", FloatType(), True),
    StructField("snow_mm", FloatType(), True),
    StructField("wind_dir_deg", FloatType(), True),
    StructField("wind_speed_kmh", FloatType(), True),
    StructField("pressure_hpa", FloatType(), True),
    StructField("airport_id", StringType(), True),
])

# df_original = spark.read.csv(f"s3://{input_bucket}/weather_meteo_by_airport.csv", header=True, schema=schema)
df_original = spark.read.csv("/home/hadoop/workspace/src/weather_meteo_by_airport.csv", header=False, schema=schema)

df_transformed = df_original.withColumn("airport_id", upper(trim(col("airport_id")))) \
    .withColumn("processed_timestamp", current_timestamp())

df_clean = df_transformed.dropna(subset=["weather_date", "airport_id"])
df_final = df_clean.dropDuplicates(["weather_date", "airport_id"])
df_sorted = df_final.orderBy("weather_date", "airport_id")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}")
df_sorted.writeTo(f"s3tablesbucket.{namespace}.weather") \
  .using("Iceberg") \
  .tableProperty("format-version", "2") \
  .createOrReplace()

job.commit()