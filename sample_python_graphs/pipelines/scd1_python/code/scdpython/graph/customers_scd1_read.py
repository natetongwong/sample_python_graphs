from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scdpython.config.ConfigStore import *
from scdpython.udfs.UDFs import *

def customers_scd1_read(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/tmp/ntong/python_sample/customers_scd1")
