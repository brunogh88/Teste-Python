from pyspark.sql import functions as F
class GenerateProfitProcess(object):
    
    def __init__(self):
        pass

    def process(self, df_contract_trans):
        return (
            df_contract_trans
            .withColumn("vl_profit", (F.col("percentage")/100) * F.col("vl_net_amount"))
        )