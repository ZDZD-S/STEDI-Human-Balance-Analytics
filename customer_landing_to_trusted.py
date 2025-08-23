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

# Read customer data from landing zone
customer_landing_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://zsmbucket360/customer/landing/"], "recurse": True},
    transformation_ctx="customer_landing_df",
)

# Explicitly map the serialnumber field to a string type
mapped_df = ApplyMapping.apply(
    frame=customer_landing_df,
    mappings=[
        ("serialnumber", "string", "serialnumber", "string"),
        ("email", "string", "email", "string"),
        ("customerName", "string", "customerName", "string"),
        ("phone", "string", "phone", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
    ],
    transformation_ctx="mapped_df",
)

# Write to the trusted zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=mapped_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://zsmbucket360/customer/trusted/", "partitionKeys": []},
    transformation_ctx="write_to_customer_trusted",
)

job.commit()