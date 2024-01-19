from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from scdpython.udfs.UDFs import *

def RowDistributor_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    df1 = in0.filter((col("random_id") == lit(0)))
    df2 = in0.filter((col("random_id") == lit(1)))
    df3 = in0.filter((col("random_id") == lit(2)))

    return df1, df2, df3
