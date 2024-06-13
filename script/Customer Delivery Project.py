import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer
Customer_node1718284157475 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-customer-delivery/staging/customer.csv"], "recurse": True}, transformation_ctx="Customer_node1718284157475")

# Script generated for node Delivery
Delivery_node1718284183696 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-customer-delivery/staging/delivery.csv"], "recurse": True}, transformation_ctx="Delivery_node1718284183696")

# Script generated for node Data Type Change
DataTypeChange_node1718285479620 = ApplyMapping.apply(frame=Customer_node1718284157475, mappings=[("id", "string", "id", "int"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("age", "string", "age", "int"), ("city", "string", "city", "string"), ("email", "string", "email", "string")], transformation_ctx="DataTypeChange_node1718285479620")

# Script generated for node Data Type Change
DataTypeChange_node1718285614203 = ApplyMapping.apply(frame=Delivery_node1718284183696, mappings=[("order_id", "string", "order_id", "int"), ("order_timestamp", "string", "order_timestamp", "string"), ("delivered_timestamp", "string", "delivered_timestamp", "string"), ("driver_id", "string", "driver_id", "int"), ("restaurant_id", "string", "restaurant_id", "int"), ("cust_id", "string", "cust_id", "int"), ("delivery_region", "string", "delivery_region", "string"), ("discount_applied", "string", "discount_applied", "boolean"), ("discount_code", "string", "discount_code", "string"), ("order_total", "string", "order_total", "double"), ("discount_pc", "string", "discount_pc", "decimal"), ("status", "string", "status", "string")], transformation_ctx="DataTypeChange_node1718285614203")

# Script generated for node Filter - Age
FilterAge_node1718285504145 = Filter.apply(frame=DataTypeChange_node1718285479620, f=lambda row: (row["age"] > 15 and row["age"] < 100), transformation_ctx="FilterAge_node1718285504145")

# Script generated for node Filter - Completed Order
SqlQuery4 = '''
select * from delivery
WHERE status = 'COMPLETED';
'''
FilterCompletedOrder_node1718285696070 = sparkSqlQuery(glueContext, query = SqlQuery4, mapping = {"delivery":DataTypeChange_node1718285614203}, transformation_ctx = "FilterCompletedOrder_node1718285696070")

# Script generated for node Join - Delivery&Customer
JoinDeliveryCustomer_node1718285790872 = Join.apply(frame1=FilterCompletedOrder_node1718285696070, frame2=FilterAge_node1718285504145, keys1=["cust_id"], keys2=["id"], transformation_ctx="JoinDeliveryCustomer_node1718285790872")

# Script generated for node Destination
Destination_node1718285867375 = glueContext.write_dynamic_frame.from_options(frame=JoinDeliveryCustomer_node1718285790872, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-customer-delivery/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1718285867375")

job.commit()