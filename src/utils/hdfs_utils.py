
class HdfsUtils(object):

    def __init__(self, spark_session):
        self.spark_session = spark_session


    def readHdfs(self, path, schema, format):
        if(format == "csv"):
            return ( 
                self.spark_session
                .read
                .schema(schema)
                .format(format)
                .option("sep", ";")
                .option("header", "true")
                .load(path)
            )
        else:
            return ( 
                self.spark_session
                .read
                .schema(schema)
                .format(format)
                .load(path)
            )

    def readOracle(self, format, table_name):
        return (            
            self.spark_session.read 
                .format(format) 
                .option("url", "jdbc:oracle:thin:DW/test@//localhost:1521/xe") 
                .option("dbtable", table_name) 
                .option("user", "DW") 
                .option("password", "test") 
                .option("driver", "oracle.jdbc.driver.OracleDriver") 
                .load()
        )

    def write(self, df, path, format, partition_name, save_mode):
        (
            df
            .write
            .mode(save_mode)
            .format(format)
            .partitionBy(partition_name)
            .save(path)
        )

    def writeOracle(self, df, format, save_mode, table_name):
        (
            df
            .write
            .format(format)
            .option("batchsize", 1000)
            .mode(save_mode)
            .jdbc("jdbc:oracle:thin:@localhost:1521/xe",
                  table_name,
                  properties={
                      "driver": "oracle.jdbc.OracleDriver",
                      "user": "DW",
                      "password": "test"
                      }
                  )
        )