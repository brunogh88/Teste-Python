from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config
from src.utils import log

class ProfitIngest(object):
    
    def __init__(self):
        pass

    def save(self, df):
        log.info("Save as parquet")
        HdfsUtils(None).write(
            df=df, 
            path=config("PATH_PARQUET_RESULT"),
            format=config("PARQUET_FORMAT"),
            partition_name="dt_partition",
            save_mode=config("OVERWRITE_MODE")
            )
