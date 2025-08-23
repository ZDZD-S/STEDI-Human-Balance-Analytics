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

# Read customer data from trusted zone
customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_df",
)

# Filter for customers who have a `serialnumber` and keep the rest of the columns
customer_curated_df = Filter.apply(
    frame=customer_trusted_df,
    f=lambda row: row["serialnumber"] is not None,
    transformation_ctx="customer_curated_df",
)

# Write to the curated zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_customer_curated",
)

job.commit()