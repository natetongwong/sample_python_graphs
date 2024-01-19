from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from scd2_merge.udfs.UDFs import *

def SchemaTransform_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("customer_name", expr("if((rand() > 0.5D), upper(customer_name), lower(customer_name))"))
