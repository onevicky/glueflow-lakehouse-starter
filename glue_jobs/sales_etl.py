import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE", "SOURCE_TABLE", "TARGET_S3_PATH"])
context = GlueContext(SparkContext.getOrCreate())
job = Job(context)
job.init(args["JOB_NAME"], args)

source = context.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"], table_name=args["SOURCE_TABLE"], transformation_ctx="source"
)
mapped = ApplyMapping.apply(frame=source, mappings=[
    ("order_id", "string", "order_id", "string"),
    ("customer_id", "string", "customer_id", "string"),
    ("order_timestamp", "timestamp", "order_timestamp", "timestamp"),
    ("amount", "double", "amount", "double"),
])

orders = (mapped.toDF()
    .filter(F.col("order_id").isNotNull() & (F.col("amount") >= 0))
    .withColumn("order_date", F.to_date("order_timestamp"))
    .withColumn("ingested_at", F.current_timestamp())
    .dropDuplicates(["order_id"]))

context.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(orders, context, "orders"),
    connection_type="s3",
    connection_options={"path": args["TARGET_S3_PATH"], "partitionKeys": ["order_date"]},
    format="glueparquet",
)
job.commit()
