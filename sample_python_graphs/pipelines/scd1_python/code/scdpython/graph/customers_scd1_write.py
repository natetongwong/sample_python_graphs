from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scdpython.config.ConfigStore import *
from scdpython.udfs.UDFs import *

def customers_scd1_write(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if (
        DeltaTable.isDeltaTable(spark, "dbfs:/tmp/ntong/python_sample/customers_scd1")
        and spark._jvm.org.apache.hadoop.fs.FileSystem\
          .get(spark._jsc.hadoopConfiguration())\
          .exists(spark._jvm.org.apache.hadoop.fs.Path("dbfs:/tmp/ntong/python_sample/customers_scd1"))
    ):
        DeltaTable\
            .forPath(spark, "dbfs:/tmp/ntong/python_sample/customers_scd1")\
            .alias("target")\
            .merge(in0.alias("source"), (col("source.customer_id") == col("target.customer_id")))\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/tmp/ntong/python_sample/customers_scd1")
