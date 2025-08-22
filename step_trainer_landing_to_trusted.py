import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read Step Trainer data from the landing zone
step_trainer_landing_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zsmbucket360/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_df",
)

# Filter for privacy-consenting customers
customer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "paths": ["s3://zsmbucket360/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_df",
)

# Join Step Trainer with trusted customer data to filter out non-consenting users
joined_df = Join.apply(
    frame1=step_trainer_landing_df,
    frame2=customer_trusted_df,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="joined_df",
)

# Write to the trusted zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=joined_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_step_trainer_trusted",
)

job.commit()