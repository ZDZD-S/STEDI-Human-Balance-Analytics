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


# Read Step Trainer and Customer data from the landing zone
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

customer_landing_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zsmbucket360/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_df",
)


# Filter customers who have shared their data for research
customer_trusted_filtered_df = Filter.apply(
    frame=customer_landing_df,
    f=lambda row: (row["sharewithresearchasofdate"] is not None),
    transformation_ctx="customer_trusted_filtered_df",
)


# Join Step Trainer data with the filtered Customer data on serial number
joined_df = Join.apply(
    frame1=step_trainer_landing_df,
    frame2=customer_trusted_filtered_df,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="joined_df",
)


# Drop redundant customer fields to protect privacy
final_step_trainer_trusted_df = DropFields.apply(
    frame=joined_df,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="final_step_trainer_trusted_df",
)


# Write the trusted Step Trainer data to the trusted zone in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=final_step_trainer_trusted_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/step_trainer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="write_to_trusted_zone",
)


job.commit()