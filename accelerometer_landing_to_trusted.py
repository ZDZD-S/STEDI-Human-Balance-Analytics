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

# Read accelerometer data from landing zone
accelerometer_landing_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zsmbucket360/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_df",
)

# Read trusted customer data
customer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "paths": ["s3://zsmbucket360/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_df",
)

# Join to filter for privacy-consenting customers
privacy_filtered_df = Join.apply(
    frame1=accelerometer_landing_df,
    frame2=customer_trusted_df,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="privacy_filtered_df",
)

# Write to the trusted zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=privacy_filtered_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_accelerometer_trusted",
)

job.commit()