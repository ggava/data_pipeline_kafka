import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "pedidos")
    .option("startingOffsets", "earliest")
    .load()
)

df_json = df_kafka.select(
    from_json(
        col("value").cast("string"),\
        schema_pedidos
    ).alias("pedido")
)

df_pedidos = df_json.select("pedido.*")

motivos_invalidade = concat_ws(
    "; ",

    when(
        col("pedido_id").isNull(),
        lit("pedido_id ausente")
    ),

    when(
        col("produto_id").isNull(),
        lit("produto_id ausente")
    ),

    when(
        col("produto_id") <= 0,
        lit("produto_id deve ser maior que zero")
    ),

    when(
        col("produto").isNull() | (trim(col("produto")) == ""),
        lit("produto ausente")
    ),

    when(
        col("quantidade").isNull(),
        lit("quantidade ausente")
    ),

    when(
        col("quantidade") <= 0,
        lit("quantidade deve ser maior que zero")
    ),

    when(
        col("quantidade") > 10,
        lit("quantidade excede o limite permitido")
    ),

    when(
        col("valor_total").isNull(),
        lit("valor_total ausente")
    ),

    when(
        col("valor_total") <= 0,
        lit("valor_total deve ser maior que zero")
    ),

    when(
        col("timestamp").isNull() | (trim(col("timestamp")) == ""),
        lit("timestamp ausente")
    )
)

df_classificado = (
    df_pedidos
    .withColumn(
        "motivos_invalidade",
        motivos_invalidade
    )
    .withColumn(
        "status",
        when(
            col("motivos_invalidade") == "",
            lit("VALIDO")
        ).otherwise(lit("INVALIDO"))
    )
)

df_validos = df_classificado.filter(
    col("status") == "VALIDO"
)

df_invalidos = df_classificado.filter(
    col("status") == "INVALIDO"
)

print("\n========== SCHEMA DOS PEDIDOS ==========\n")

df_classificado.printSchema()

query_validos = (
    df_validos.writeStream
    .queryName("pedidos_validos")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query_invalidos = (
    df_invalidos.writeStream
    .queryName("pedidos_invalidos")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

spark.streams.awaitAnyTermination()
