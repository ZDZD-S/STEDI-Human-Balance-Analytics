import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read step trainer trusted data
step_trainer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_df",
)

# Read curated customer data
customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="curated",
    transformation_ctx="customer_curated_df",
)

# Read trusted accelerometer data
accelerometer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_df",
)

# Convert DynamicFrames to Spark DataFrames for a correct join
step_trainer_spark_df = step_trainer_trusted_df.toDF()
customer_curated_spark_df = customer_curated_df.toDF()
accelerometer_spark_df = accelerometer_trusted_df.toDF()

# Rename the 'email' column to avoid ambiguity in the final join.
# This change is only temporary in memory and does not affect your S3 files or Athena tables.
customer_curated_spark_df = customer_curated_spark_df.withColumnRenamed("email", "customer_email")

# Join all three DataFrames to filter and match data
joined_data = step_trainer_spark_df.join(
    customer_curated_spark_df,
    on="serialnumber",
    how="inner"
).join(
    accelerometer_spark_df,
    (col("customer_email") == col("user")) & ((col("sensorReadingTime") / 1000).cast("long") == (col("timestamp") / 1000).cast("long")),
    how="inner"
)

# Convert the joined DataFrame back to a DynamicFrame
joined_data_dynamic_frame = DynamicFrame.fromDF(
    joined_data, glueContext, "joined_data_dynamic_frame"
)

# Drop redundant fields from the dataset
machine_learning_curated_df = DropFields.apply(
    frame=joined_data_dynamic_frame,
    paths=["user", "x", "y", "z"],
    transformation_ctx="machine_learning_curated_df",
)

# Write the final joined data to the curated zone
glueContext.write_dynamic_frame.from_options(
    frame=machine_learning_curated_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_machine_learning_curated",
)

job.commit()