import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from delta import *
from delta import DeltaTable



import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# The KryoSerializer is necessary for the SparkSession to be able to serialize objects


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


# Spark + Glue context configuration
spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)

# Cloud Formation Template Parameters
args = getResolvedOptions(sys.argv,
                          ['base_s3_path'])

# Simulation of Delta Lake Layers
base_s3_path = args['base_s3_path']
target_s3_path = "{base_s3_path}/tmp/delta_target".format(
    base_s3_path=base_s3_path)

table_name = "delta_table"
final_base_path = "{base_s3_path}/tmp/delta_table".format(
    base_s3_path=base_s3_path)

print("Writing delta table")
# Write table
data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save(target_s3_path)

print("Writing delta table succeeded.")

print("Reading delta table:")
# Read table
df = spark.read.format("delta").load(target_s3_path)
df.show()
print("Reading delta succeeded:")

print("Overwriting delta table with updates:")
#Update table data
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save(target_s3_path)

print("Reading after update: ")

# Reload complete table
df = spark.read.format("delta").load(target_s3_path)
df.show()

print("Reading using time travel: ")

#Reading using time travel
df = spark.read.format("delta").option("versionAsOf", 0).load(target_s3_path)
df.show()

print("Reading normally to create catalog: ")

# Reload complete table
df = spark.read.format("delta").load(target_s3_path)
df.show()

df.createOrReplaceTempView("delta_table_snapshot")
print("Time to Glue Catalog integration")

print("Creating manifest file")
deltaTable = DeltaTable.forPath(spark, target_s3_path)
deltaTable.generate("symlink_format_manifest")

print("Manifest created, writing as table:")
df.write.format("delta").mode("overwrite").option("path", target_s3_path).saveAsTable("delta_demo.delta_table_test")

print("Droping and re-creating table with spark.sql")

spark.sql("DROP TABLE IF EXISTS delta_demo.delta_table_test;")

spark.sql(
    f"CREATE TABLE IF NOT EXISTS delta_demo.delta_table_test USING PARQUET LOCATION '{target_s3_path}/_symlink_format_manifest/' as (SELECT * from delta_table_snapshot)")

print(f"Table delta_demo.delta_table_test created")

print("CREATE EXTERNAL TABLE command ran successfully")
