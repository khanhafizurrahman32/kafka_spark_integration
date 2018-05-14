from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import *
from sklearn.preprocessing import LabelEncoder
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'

enc = LabelEncoder()

@fn.udf
def to_upper(s):
    if s is not None:
        return s.upper()

def to_list_check(x,s):
    x.append(s)
    return x

#to_upper = udf(to_upper_string)
to_list = fn.udf(to_list_check)

def use_label_encoder(label_encoder, y):
    return label_encoder.transform(y)

to_transform_class_val = fn.udf(use_label_encoder, IntegerType())

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
df1 = df.select(fn.from_json(df.value, schema).alias("json"))
df1.createOrReplaceTempView("irisDataSet")
sqldf = spark.sql("SELECT * FROM irisDataSet")


show_vals = df1.select('json.sepal_length_in_cm', 'json.sepal_width_in_cm')
with_columns = df1.withColumn('json.class', to_upper('json.class')).alias('tr')
class_val_count = df1.select(
    fn.explode(
        fn.array("json.class")
    ).alias("classes")
)


#collect_val = class_val_count.groupBy().collect()
mean_val_calc = df1.groupBy("json.class").agg({"json.sepal_length_in_cm": "mean" ,
                                                 "json.sepal_width_in_cm": "mean",
                                                 "json.petal_length_in_cm": "mean",
                                                 "json.petal_width_in_cm": "mean"})





#df1 = df1.withColumn('new_col', to_transform_class_val('json.class'))
#df1 = df1.withColumn('label_enc', col('new_col') + lit(1))


print("DataFrame is ")

mean_val_calc.writeStream \
    .format("console") \
    .option("truncate","false") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()











