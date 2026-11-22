import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")

spark = (
    SparkSession.builder
    .appName("PedidosKafka")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema_pedidos = StructType([
    StructField("pedido_id", LongType(), True),
    StructField("produto_id", IntegerType(), True),
    StructField("produto", StringType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "pedidos")
    .option("startingOffsets", "earliest")
    .load()
)

df_json = df.select(
    from_json(
        col("value").cast("string"),\
        schema_pedidos
    ).alias("pedido")
)

df_pedidos = df_json.select("pedido.*")

df_pedidos.printSchema()

query = (
    df_pedidos.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()