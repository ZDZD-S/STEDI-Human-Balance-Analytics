import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the trusted Step Trainer and Accelerometer data
step_trainer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "paths": ["s3://zsmbucket360/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_df",
)

accelerometer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "paths": ["s3://zsmbucket360/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_df",
)

# Join the two trusted datasets
joined_df = Join.apply(
    frame1=step_trainer_trusted_df,
    frame2=accelerometer_trusted_df,
    keys1=["sensorreadingtime"],
    keys2=["timeStamp"],
    transformation_ctx="joined_df",
)

# Write the final curated data
glueContext.write_dynamic_frame.from_options(
    frame=joined_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/ML_curated/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_curated_zone",
)

job.commit()