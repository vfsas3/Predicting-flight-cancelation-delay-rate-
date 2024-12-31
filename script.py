from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import *
from pyspark.streaming import StreamingContext

def spark_session():
    return SparkSession.builder \
        .appName("Voos e atrasos em 2018") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/projeto") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/projeto") \
        .getOrCreate()

def batch_report(spark):
    # Load data
    df_atrasos = spark.read.format("mongo").option("collection", "atrasos").load()
    df_voos = spark.read.format("mongo").option("collection", "voos").load()

    # Batch report: Total flights and delays per airport
    airport_summary = df_atrasos.groupBy("airport", "airport_name") \
        .agg(
            sum("arr_flights").alias("total_flights"),
            sum("arr_del15").alias("total_delayed_flights"),
            avg("arr_delay").alias("avg_delay_minutes")
        )
    
    print("Airport Summary:")
    airport_summary.show()

    # Batch report: Revenue generated per airline
    revenue_summary = df_voos.groupBy("AirlineCompany") \
        .agg(
            sum(col("PricePerTicket") * col("NumTicketsOrdered")).alias("total_revenue"),
            avg("PricePerTicket").alias("avg_ticket_price")
        )
    
    print("Revenue Summary:")
    revenue_summary.show()

    # Save reports to MongoDB
    airport_summary.write.format("mongo").option("collection", "airport_summary").mode("overwrite").save()
    revenue_summary.write.format("mongo").option("collection", "revenue_summary").mode("overwrite").save()

def streaming_report(spark):
    pass

if __name__ == "__main__":
    spark = spark_session()

    batch_report(spark)

    streaming_report(spark)