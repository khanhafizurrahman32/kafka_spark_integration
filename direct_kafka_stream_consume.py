from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sklearn.preprocessing import LabelEncoder
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'

enc = LabelEncoder()

def to_upper_string(s):
    if s is not None:
        return s.upper()

to_upper = udf(to_upper_string)

def use_label_encoder(label_encoder, y):
    return label_encoder.transform(y) + 1

to_transform_class_val = udf(use_label_encoder, IntegerType())

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

label_encoder = enc.fit(df1.select("json.class"))
transform_val = df1.select(to_transform_class_val(label_encoder,"json.class"))
#select_val = df1.select("json.sepal_length_in_cm").agg({"aa": "avg"})
#sepal_length = df1.agg({"json.sepal_length_in_cm": "avg"})
#sepal_width = df1.agg({"json.sepal_width_in_cm": "avg"})
#petal_length = df1.agg({"json.petal_length_in_cm": "avg"})
#petal_width = df1.agg({"json.petal_width_in_cm": "avg"})
#select_val_list = [{'sepal_length_in_cm': sepal_length, 'sepal_width_in_cm': sepal_width, 'petal_length_in_cm': petal_length, 'petal_width_in_cm': petal_width}]
#class_val = df1.select('json.class',udf(to_upper(col("json.class"))))
#select_val = df1.agg({"json.sepal_length_in_cm": "avg"})
#mean_array = [sepal_length, sepal_width, petal_length, petal_width]
print("DataFrame is ")
print(df1.select("json.class"))
class_val = df1.select(to_upper("json.class"))

transform_val.writeStream \
    .format("console") \
    .option("truncate","false") \
    .start() \
    .awaitTermination()











