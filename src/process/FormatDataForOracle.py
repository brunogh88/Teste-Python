from pyspark.sql import functions as F
from pyspark.sql.types import StringType
class FormatDataForOracle(object):

    def __init__(self):
        pass

    def process(self, df_final):
        return(
            df_final
            .withColumnRenamed("dt_partition", "DT_PROCESS")
            .withColumn("DT_PROCESS", F.to_date(F.col("DT_PROCESS").cast(StringType()), "yyyyMMdd"))
        )