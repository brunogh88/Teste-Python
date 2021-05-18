from pyspark.sql import functions as F

class SumProfitProcess(object):
    
    def __init__(self):
        pass

    def process(self, df_contract_trans):
        return (
            df_contract_trans
            .select(F.round(F.sum(F.col("vl_profit")),3).alias("vl_profit"), F.max(F.col("dt_partition")).alias("dt_partition"))
        )