from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config

from src.utils import log

class ContractOracleLoad(object):
    """Contract Oracle Load

    Args:
        spark_session: Spark Session
    """
    
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def load(self):
        log.info("Load Oracle data Contracts")
        return (
            HdfsUtils(self.spark_session)
            .readOracle(format=config("JDBC_FORMAT"), table_name="TB_CONTRACT")
            .withColumnRenamed( "CONTRACT_ID", "contract_id")
            .withColumnRenamed( "CLIENT_ID", "client_id")
            .withColumnRenamed( "CLIENT_NAME", "client_name")
            .withColumnRenamed( "PERCENTAGE", "percentage")
            .withColumnRenamed( "IS_ACTIVE", "is_active")
        )

    