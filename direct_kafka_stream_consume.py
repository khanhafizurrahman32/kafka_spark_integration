from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd


spark = SparkSession \
    .builder \
    .appName("learning_01") \
    .master("local") \
    .getOrCreate()

schema_2 = StructType([
    StructField("class", StringType()),
    StructField("sepal_length_mean", DoubleType()),
    StructField("sepal_width_mean", DoubleType()),
    StructField("petal_length_mean", DoubleType()),
    StructField("petal_width_mean", DoubleType())
])

df = spark \
    .readStream \
    .schema(schema_2) \
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

@udf('string')
def to_string_vals(s):
    return s.split(",")

df = df.selectExpr("CAST(value AS STRING)")
df.printSchema()
print(type(df.select('value')))
test_val = df.select('value')
test_split_vals = test_val.withColumn('v2', to_string_vals(test_val.value))
df1 = df.select(from_json(df.value, schema).alias("json"))
df1.printSchema()



@pandas_udf(schema_2, PandasUDFType.GROUPED_MAP)
def mean_val_pandas(pdf):
    result = pd.DataFrame(pdf.groupby('json.class').apply(
        lambda  x: x.loc[:, ['json.sepal_length_in_cm',
                             'json.sepal_width_in_cm',
                             'json.petal_length_in_cm',
                             'json.petal_width_in_cm']].mean()
    ))
    result.reset_index(inplace= True, drop= False)
    return result

mean_val_pandas_calc = df1.groupby('json.class').apply(mean_val_pandas)


mean_val_calc = df1.groupBy('json.class').agg({"json.sepal_length_in_cm": "mean" ,
                                                 "json.sepal_width_in_cm": "mean",
                                                 "json.petal_length_in_cm": "mean",
                                                 "json.petal_width_in_cm": "mean"})


print("DataFrame is ")

df1.writeStream \
    .format("console") \
    .option("truncate","false") \
    .start() \
    .awaitTermination()

