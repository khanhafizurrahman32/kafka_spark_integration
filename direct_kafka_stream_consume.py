from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("learning_01") \
    .master("local") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "irisDataSetTopic") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("sepal_length_in_cm", StringType()), \
    StructField("sepal_width_in_cm", StringType()), \
    StructField("petal_length_in_cm", StringType()), \
    StructField("petal_width_in_cm", StringType()), \
    StructField("class", StringType())
])


df = df.selectExpr("CAST(value AS STRING)")
df1 = df.select(from_json(df.value, schema).alias("json"))

mean_val_calc = df1.groupBy("json.class").agg({"json.sepal_length_in_cm": "mean" ,
                                                 "json.sepal_width_in_cm": "mean",
                                                 "json.petal_length_in_cm": "mean",
                                                 "json.petal_width_in_cm": "mean"})


print("DataFrame is ")

mean_val_calc.writeStream \
    .format("console") \
    .option("truncate","false") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()