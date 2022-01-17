import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from delta import *
from delta import DeltaTable
from faker import Faker


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

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
                          ['base_s3_path', 'fake_row_count'])

# Simulation of Delta Lake Layers
base_s3_path = args['base_s3_path']
target_s3_path = "{base_s3_path}/tmp/delta_employee".format(
    base_s3_path=base_s3_path)

table_name = "delta_table"
final_base_path = "{base_s3_path}/tmp/final/delta_employee".format(
    base_s3_path=base_s3_path)

# Initialize Faker for performance evaluation
fake_row_count = int(args['fake_row_count'])

fake = Faker()

fake_workers = [(
        fake.unique.random_int(min=1, max=fake_row_count),
        fake.name(),
        fake.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
        fake.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
        fake.random_int(min=10000, max=150000),
        fake.random_int(min=18, max=60),
        fake.random_int(min=0, max=100000),
        fake.unix_time()
      ) for x in range(fake_row_count)]

table_name = "delta_employee"

columns= ["emp_id", "employee_name","department","state","salary","age","bonus","ts"]
emp_df = spark.createDataFrame(data = fake_workers, schema = columns)

print("DF from fake workers: \n")
print(emp_df.show(10))

# Write table
emp_df.write.format("delta").mode("overwrite").save(target_s3_path)

print("Writing delta table succeeded.")

print("Reading delta table:")
# Read table
emp_df_read = spark.read.format("delta").load(target_s3_path)
emp_df_read.show()
print("Reading delta succeeded:")

emp_df_read.createOrReplaceTempView("delta_employee_view")
table_name = "delta_employee"

# Create delta Table and show its results
spark.sql(f"CREATE DATABASE IF NOT EXISTS delta_demo")

spark.sql("DROP TABLE IF EXISTS delta_demo.{table_name}".format(table_name=table_name))
existing_tables = spark.sql(f"SHOW TABLES IN delta_demo;")

df_existing_tables = existing_tables.select('tableName').rdd.flatMap(lambda x:x).collect()

if table_name not in df_existing_tables:
    print("Table delta_employee does not exist in Glue Catalog. Creating it now.")

    print("Creating manifest file")
    deltaTable = DeltaTable.forPath(spark, target_s3_path)
    deltaTable.generate("symlink_format_manifest")

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta_demo.delta_employee USING PARQUET LOCATION '{target_s3_path}/_symlink_format_manifest/' as (SELECT * from delta_employee_view)")
    
# Upsert records into delta table
simpleDataUpd = [
    (3, "Gabriel","Sales","RJ",81000,30,23000,827307999), \
    (7, "Paulo","Engineering","RJ",79000,53,15000,1627694678), \
  ]

columns= ["emp_id", "employee_name","department","state","salary","age","bonus","ts"]
emp_up_df = spark.createDataFrame(data = simpleDataUpd, schema = columns)


print("Employee Updates:")
print(emp_up_df.show())

emp_up_df.createOrReplaceTempView("delta_employee_updt")

print("Merge into delta Table:")
empDeltaTable = DeltaTable.forPath(spark, target_s3_path)

empDeltaTable.alias("employee_table").merge(
    emp_up_df.alias("updates"),
    "employee_table.emp_id = updates.emp_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

print("Merge Into delta table started")
empDeltaTable.toDF().createOrReplaceTempView("delta_employee_view_updt")
spark.sql("DROP TABLE IF EXISTS delta_demo.{table_name}".format(table_name=table_name))
empDeltaTable.generate("symlink_format_manifest")
spark.sql(f"CREATE TABLE IF NOT EXISTS delta_demo.delta_employee USING PARQUET LOCATION '{target_s3_path}/_symlink_format_manifest/' as (SELECT * from delta_employee_view_updt)")

print("Merge Into delta table success - check results:\n")

delta_merge_df = spark.sql(f"SELECT * FROM delta_demo.delta_employee where emp_id = 3 or emp_id = 7")
print("Delta Table Results after merge:\n")
print(delta_merge_df.show())