from pyspark.sql import functions as F

class GenerateNetValueTransactionProcess(object):
    
    def __init__(self):
        pass

    def process(self, df_transaction):
        return (
            df_transaction
            .withColumn("vl_net_amount", 
                        F.col("total_amount") - ((F.col("discount_percentage")/100) * F.col("total_amount")))
        )