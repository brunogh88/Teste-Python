from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config
from src.model.contract_struct import contract_struct
from src.utils import log

class ContractLoad(object):
    """Contract Load

    Args:
        spark_session: Spark Session
    """
    
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def load(self):
        log.info("Load data Contracts")
        return HdfsUtils(self.spark_session).readHdfs(
                    path=config("PATH_CSV_CONTRACT"),
                    schema=contract_struct(),
                    format=config("CSV_FORMAT")
        )

    