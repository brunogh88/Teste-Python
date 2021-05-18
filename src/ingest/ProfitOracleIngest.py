from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config

class ProfitOracleIngest(object):

    def __init__(self):
        pass

    def save(self, df_final):
        (
            HdfsUtils(None)
            .writeOracle(df=df_final, format=config("JDBC_FORMAT"), save_mode=config("APPEND_MODE"), table_name="TB_FINAL")
        )
        