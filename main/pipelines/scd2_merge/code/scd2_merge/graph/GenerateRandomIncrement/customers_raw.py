from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from scd2_merge.udfs.UDFs import *

def customers_raw(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", IntegerType(), True), StructField("tax_id", DoubleType(), True), StructField("tax_code", StringType(), True), StructField("customer_name", StringType(), True), StructField("state", StringType(), True), StructField("city", StringType(), True), StructField("postcode", StringType(), True), StructField("street", StringType(), True), StructField("number", StringType(), True), StructField("unit", StringType(), True), StructField("region", StringType(), True), StructField("district", StringType(), True), StructField("lon", DoubleType(), True), StructField("lat", DoubleType(), True), StructField("ship_to_address", StringType(), True), StructField("valid_from", IntegerType(), True), StructField("valid_to", DoubleType(), True), StructField("units_purchased", DoubleType(), True), StructField("loyalty_segment", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/databricks-datasets/retail-org/customers/customers.csv")
