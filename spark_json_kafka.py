from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import udf,col
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

if __name__ == '__main__':
    
    spark_cassandra = SparkSession.builder.appName('PySpark-App').getOrCreate()
    spark_cassandra.conf.set("spark.cassandra.connection.host", "172.17.0.2,172.17.0.3")


    spark_kafka = SparkSession.builder.appName('PySpark-App').getOrCreate()
    df_kafka = spark_kafka.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "big-data-analytics") \
    .option("startingOffsets", "earliest") \
    .load()


    res = df_kafka.selectExpr("CAST(value AS STRING)")
    res.writeStream.outputMode("append").format("memory").queryName("table").start()

    parseSchema = StructType((
        StructField("id",IntegerType(),True),
        StructField("nm",StringType(),True),
        StructField("cty",StringType(),True),
        StructField("hse",StringType(),True),
        StructField("yrs",StringType(),True)))

    while True:
        time.sleep(5)
        df = spark_kafka.sql("select * from table")
        df = df.select(from_json(col("value"), parseSchema).alias("n"))
        df = df.selectExpr("n.id","n.nm","n.cty","n.hse","n.yrs")
        df.write.format("org.apache.spark.sql.cassandra").options(table="users", keyspace = "emp").save(mode ="append")

