from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from scd2_merge.udfs.UDFs import *

def DropRandomId(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.drop("random_id")
