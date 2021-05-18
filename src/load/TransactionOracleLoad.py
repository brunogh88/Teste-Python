from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config
from src.utils import log

class TransactionOracleLoad(object):
    """Transaction Oracle Load

    Args:
        spark_session: Spark Session
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def load(self):
        log.info("Load Oracle data Transactions")
        return (
            HdfsUtils(self.spark_session)
            .readOracle(format=config("JDBC_FORMAT"), table_name="TB_TRANSACTION")
            .withColumnRenamed( "TRANSACTION_ID", "transaction_id")
            .withColumnRenamed( "CLIENT_ID", "client_id")
            .withColumnRenamed( "TOTAL_AMOUNT", "total_amount")
            .withColumnRenamed( "DISCOUNT_PERCENTAGE", "discount_percentage")
        )