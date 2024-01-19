from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from scdpython.udfs.UDFs import *

def SelectFields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("customer_id"), col("tax_id"), col("tax_code"), col("customer_name"), col("state"))
