from pyspark.sql import functions as F
class TransactionQuality(object):

    def __init__(self):
        pass

    def process(self, df_transaction):
        return(
            df_transaction
            .withColumn("discount_percentage", F.when(F.col("discount_percentage").isNull(), F.lit(0)).otherwise(F.col("discount_percentage")))
        )