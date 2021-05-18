from pyspark.sql import functions as F
from src.load.ContractOracleLoad import ContractOracleLoad
from src.load.TransactionOracleLoad import TransactionOracleLoad
from src.quality.TransactionQuality import TransactionQuality
from src.process.SelectContractsActiveProcess import SelectContractsActiveProcess
from src.process.GenerateNetValueTransactionProcess import GenerateNetValueTransactionProcess
from src.process.UnionContractTransactionProcess import UnionContractTransactionProcess
from src.process.GenerateProfitProcess import GenerateProfitProcess
from src.process.SumProfitProcess import SumProfitProcess
from src.process.FormatDataForOracle import FormatDataForOracle
from src.ingest.ProfitIngest import ProfitIngest
from src.ingest.ProfitOracleIngest import ProfitOracleIngest

class OraclePipe(object):
    """Class with steps pipe Oracle"""

    def __init__(self, spark, args):
        self.spark_session = spark
        self.args = args
        self.df_contract = None
        self.df_transaction = None
        self.df_contract_trans = None
        self.df_final = None
        self.df_oracle = None

    def __loadStep(self):
        self.df_contract = ContractOracleLoad(self.spark_session).load().withColumn("dt_partition", F.lit(self.args.date))
        self.df_transaction = TransactionOracleLoad(self.spark_session).load().withColumn("dt_partition", F.lit(self.args.date))

    def __quality(self):
        self.df_transaction = TransactionQuality().process(self.df_transaction)

    def __processStep(self):
        self.df_contract = SelectContractsActiveProcess().process(self.df_contract)
        self.df_transaction = GenerateNetValueTransactionProcess().process(self.df_transaction)
        self.df_contract_trans = UnionContractTransactionProcess().process(self.df_contract, self.df_transaction)
        self.df_contract_trans = GenerateProfitProcess().process(self.df_contract_trans)
        self.df_final = SumProfitProcess().process(self.df_contract_trans)
        self.df_oracle = FormatDataForOracle().process(self.df_final)

    def __ingestStep(self):
        ProfitIngest().save(self.df_final)
        ProfitOracleIngest().save(self.df_oracle)

    def start(self):
        self.__loadStep()
        self.__quality()
        self.__processStep()
        self.__ingestStep()
