from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_merge.config.ConfigStore import *
from scd2_merge.udfs.UDFs import *

def customers_scd2_1(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/FileStore/tables/ntong/python_demo/scd2/")
