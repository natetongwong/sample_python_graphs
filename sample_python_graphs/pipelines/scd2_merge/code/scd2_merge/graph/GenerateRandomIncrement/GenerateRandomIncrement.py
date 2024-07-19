from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from scd2_merge.udfs.UDFs import *
from . import *
from .config import *

def GenerateRandomIncrement(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_customers_raw = customers_raw(spark)
    df_SelectFields = SelectFields(spark, df_customers_raw)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SelectFields)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Deduplicate_1)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1, df_RowDistributor_1_out2 = RowDistributor_1(
        spark, 
        df_SchemaTransform_1
    )
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_RowDistributor_1_out1)
    df_SchemaTransform_3 = SchemaTransform_3(spark, df_RowDistributor_1_out2)
    df_SetOperation_1 = SetOperation_1(spark, df_RowDistributor_1_out0, df_SchemaTransform_2, df_SchemaTransform_3)
    df_DropRandomId = DropRandomId(spark, df_SetOperation_1)
    df_Deduplicate_2 = Deduplicate_2(spark, df_DropRandomId)
    subgraph_config.update(Config)

    return df_Deduplicate_2
