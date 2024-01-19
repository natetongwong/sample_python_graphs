from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_merge.config.ConfigStore import *
from scd2_merge.udfs.UDFs import *

def customers_scd2(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if (
        DeltaTable.isDeltaTable(spark, "dbfs:/FileStore/tables/ntong/python_demo/scd2/")
        and spark._jvm.org.apache.hadoop.fs.FileSystem\
          .get(spark._jsc.hadoopConfiguration())\
          .exists(spark._jvm.org.apache.hadoop.fs.Path("dbfs:/FileStore/tables/ntong/python_demo/scd2/"))
    ):
        updatesDF = in0.withColumn("is_old_value", lit("true")).withColumn("is_current", lit("true"))
        updateColumns = updatesDF.columns
        existingTable = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/ntong/python_demo/scd2/")
        existingDF = existingTable.toDF()
        cond = None
        scdHistoricColumns = ["tax_id", "tax_code", "customer_name", "state"]

        for scdCol in scdHistoricColumns:
            if cond is None:
                cond = (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol]))
            else:
                cond = (cond | (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol])))

        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["customer_id"])\
                              .where((existingDF["is_current"] == lit("true")) & (cond))\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("is_old_value", lit("false"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("customer_id")))
        updateCond = None

        for scdCol in scdHistoricColumns:
            if updateCond is None:
                updateCond = (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol]))
            else:
                updateCond = (updateCond | (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol])))

        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["customer_id"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["is_current"] == lit("true")) & updateCond,
              set = {
"is_current" : "false", "end_time" : "staged_updates.from_time"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/ntong/python_demo/scd2/")
