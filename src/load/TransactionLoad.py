from src.utils import hdfs_utils
from src.utils.config import config
from src.model.transaction_struct import transaction_struct
from src.utils import log

class TransactionLoad(object):
    """Transaction Load

    Args:
        spark_session: Spark Session
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.hdfs_utils = hdfs_utils.HdfsUtils(spark_session)

    def load(self):
        log.info("Load data Transactions")
        return self.hdfs_utils.readHdfs(
            config("PATH_CSV_TRANSACTION"), 
            transaction_struct(), config("CSV_FORMAT")
            )