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

# Join the two datasets on the serialnumber field
joined_data = Join.apply(
    frame1=step_trainer_trusted_df,
    frame2=customer_curated_df,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="joined_data",
)

# Drop redundant fields from the curated customer data
machine_learning_curated_df = DropFields.apply(
    frame=joined_data,
    paths=["email", "birthDay", "customerName", "phone", "registrationDate", "shareWithFriendsAsOfDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "lastUpdateDate"],
    transformation_ctx="machine_learning_curated_df",
)

# Write the final curated data to the correct ML curated zone
glueContext.write_dynamic_frame.from_options(
    frame=machine_learning_curated_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://zsmbucket360/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="write_to_ml_curated",
)

job.commit()