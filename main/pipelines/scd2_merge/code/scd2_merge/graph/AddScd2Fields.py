from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_merge.config.ConfigStore import *
from scd2_merge.udfs.UDFs import *

def AddScd2Fields(spark: SparkSession, GenerateRandomIncrement: DataFrame) -> DataFrame:
    return GenerateRandomIncrement\
        .withColumn("from_time", current_timestamp())\
        .withColumn("end_time", lit(None).cast(TimestampType()))\
        .withColumn("is_current", lit(True))\
        .withColumn("is_old_value", lit(False))
