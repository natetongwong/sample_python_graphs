from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from scdpython.config.ConfigStore import *
from scdpython.udfs.UDFs import *
from prophecy.utils import *
from scdpython.graph import *

def pipeline(spark: SparkSession) -> None:
    df_GenerateRandomIncrement = GenerateRandomIncrement(spark, Config.GenerateRandomIncrement)
    customers_scd1_write(spark, df_GenerateRandomIncrement)
    df_customers_scd1_read = customers_scd1_read(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/scd1_python")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/scd1_python", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/scd1_python")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
