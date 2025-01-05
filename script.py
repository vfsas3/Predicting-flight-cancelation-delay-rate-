from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

def spark_():
    return SparkSession.builder \
        .appName("Voos e Atrasos em 2018") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/projeto") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/projeto") \
        .getOrCreate()

def batch(spark):
    df_atrasos = spark.read.format("mongo").option("collection", "atrasos").load()
    df_voos = spark.read.format("mongo").option("collection", "voos").load()

    airport_resumo = df_atrasos.groupBy("airport", "airport_name") \
        .agg(
            sum("arr_flights").alias("total_flights"),
            sum("arr_del15").alias("total_delayed_flights"),
            avg("arr_delay").alias("avg_delay_minutes")
        )
    print("Resumo dos Aeroportos:")
    airport_resumo.show()

    revenue_resumo = df_voos.groupBy("AirlineCompany") \
        .agg(
            sum(col("PricePerTicket") * col("NumTicketsOrdered")).alias("total_revenue"),
            avg("PricePerTicket").alias("avg_ticket_price")
        )
    print("Resumo de Rendimentos por Companhia Aérea:")
    revenue_resumo.show()

    airport_resumo.write.format("mongo").option("collection", "batch_aeroporto_resumo").mode("overwrite").save()
    revenue_resumo.write.format("mongo").option("collection", "batch_rendimento_resumo").mode("overwrite").save()

def streaming(spark):
    df_atrasos = spark.read.format("mongo").option("collection", "atrasos").load()
    df_voos = spark.read.format("mongo").option("collection", "voos").load()

    df_atrasos = df_atrasos.filter(df_atrasos["year"] == 2018)
    df_atrasos = df_atrasos.filter((df_atrasos["month"] >= 7) & (df_atrasos["month"] <= 9))

    df_voos = df_voos.toDF(*[col.lower() for col in df_voos.columns])

    airport_resumo_incremental = df_atrasos.groupBy("airport", "airport_name") \
        .agg(
            sum("arr_flights").alias("total_flights"),
            sum("arr_del15").alias("total_delayed_flights"),
            avg("arr_delay").alias("avg_delay_minutes")
        )
    print("Resumo Incremental dos Aeroportos:")
    airport_resumo_incremental.show()

    revenue_resumo_incremental = df_voos.groupBy("AirlineCompany") \
        .agg(
            sum(col("PricePerTicket") * col("NumTicketsOrdered")).alias("total_revenue"),
            avg("PricePerTicket").alias("avg_ticket_price")
        )
    print("Resumo Incremental de Rendimentos por Companhia Aérea:")
    revenue_resumo_incremental.show()

    airport_resumo_incremental.write.format("mongo").option("collection", "streaming_aeroporto_resumo").mode("append").save()
    revenue_resumo_incremental.write.format("mongo").option("collection", "streaming_rendimento_resumo").mode("append").save()

if __name__ == "__main__":
    spark = spark_()

    batch(spark)

    streaming(spark)
