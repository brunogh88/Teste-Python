from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType

def contract_struct():
    return StructType([
        StructField("contract_id", IntegerType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("client_name", StringType(), True),
        StructField("percentage", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
    ])