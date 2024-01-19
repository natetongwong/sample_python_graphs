from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from scd2_merge.config.ConfigStore import *
from scd2_merge.udfs.UDFs import *
from prophecy.utils import *
from scd2_merge.graph import *

def pipeline(spark: SparkSession) -> None:
    df_GenerateRandomIncrement = GenerateRandomIncrement(spark, Config.GenerateRandomIncrement)
    df_AddScd2Fields = AddScd2Fields(spark, df_GenerateRandomIncrement)
    customers_scd2(spark, df_AddScd2Fields)
    df_customers_scd2_1 = customers_scd2_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/scd2_merge")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/scd2_merge", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/scd2_merge")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
