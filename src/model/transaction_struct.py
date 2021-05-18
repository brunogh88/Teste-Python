from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType

def transaction_struct():
    return StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("total_amount", IntegerType(), True),
        StructField("discount_percentage", DoubleType(), True)
    ])