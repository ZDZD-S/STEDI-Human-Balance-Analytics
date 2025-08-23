import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame # <--- This is the missing line

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read step trainer data from landing zone
step_trainer_landing_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_df",
)

# Read curated customer data
customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="curated",
    transformation_ctx="customer_curated_df",
)

# Convert DynamicFrames to Spark DataFrames for explicit type casting
step_trainer_spark_df = step_trainer_landing_df.toDF()
customer_curated_spark_df = customer_curated_df.toDF()

# Explicitly cast the serialnumber column to string type for a robust join
step_trainer_spark_df = step_trainer_spark_df.withColumn(
    "serialnumber", col("serialnumber").cast("string")
)
customer_curated_spark_df = customer_curated_spark_df.withColumn(
    "serialnumber", col("serialnumber").cast("string")
)

# Join the two DataFrames on the serialnumber
joined_data = step_trainer_spark_df.join(
    customer_curated_spark_df,
    on="serialnumber",
    how="inner"
)

# Convert the joined DataFrame back to a DynamicFrame
joined_data_dynamic_frame = DynamicFrame.fromDF(
    joined_data, glueContext, "joined_data_dynamic_frame"
)

# Write to the trusted zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=joined_data_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_step_trainer_trusted",
)

job.commit()