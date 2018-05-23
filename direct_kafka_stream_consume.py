from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.preprocessing import LabelEncoder


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
    StructField("sepal_length_in_cm", DoubleType()), \
    StructField("sepal_width_in_cm", DoubleType()), \
    StructField("petal_length_in_cm", DoubleType()), \
    StructField("petal_width_in_cm", DoubleType()), \
    StructField("class", StringType()), \
    StructField("emni", StringType())
])

myschema = StructType([
    #StructField("class", StringType()),
    StructField("mean_sepal_length", DoubleType()),
    StructField("mean_sepal_width", DoubleType()),
    StructField("mean_petal_length", DoubleType()),
    StructField("mean_petal_width", DoubleType()),
])

output_of_scikit_learn = StructType([
    StructField("c1", DoubleType()),
    StructField("c2", DoubleType()),
])


df = df.selectExpr("CAST(value AS STRING)")
df.printSchema()
df1 = df.select(from_json(df.value, schema).alias("json"))
df1.printSchema()
df2 = df1.select('json.*')


def calculate_mean(x):
    return x.loc[:, ["sepal_length_in_cm", "sepal_width_in_cm", "petal_length_in_cm", "petal_width_in_cm"]].mean()



@pandas_udf(output_of_scikit_learn, functionType=PandasUDFType.GROUPED_MAP)
def g(df):
    """result = pd.DataFrame(df.groupby('emni').apply(
        lambda x: calculate_mean(x)
    ))"""
    X1 = pd.DataFrame(df['sepal_length_in_cm'])
    X2 = pd.DataFrame(df['sepal_width_in_cm'])
    X3 = pd.DataFrame(df['petal_length_in_cm'])
    X4 = pd.DataFrame(df['petal_width_in_cm'])
    Y = pd.DataFrame(df['class'])
    y = np.ravel(Y.values)
    enc = LabelEncoder()
    label_encoder = enc.fit(y)
    y = label_encoder.transform(y) + 1
    X = pd.concat([X1, X2, X3, X4], axis=1, ignore_index=True)
    print(X.head())
    print(y.shape)
    sklearn_lda = LDA()
    X_lda_sklearn = sklearn_lda.fit_transform(X,y)
    #result.reset_index(inplace=True, drop=False)
    return pd.DataFrame(X_lda_sklearn)

df3 = df2.groupby("emni").apply(g)


mean_val_calc = df1.groupBy('json.class').agg({"json.sepal_length_in_cm": "mean" ,
                                                 "json.sepal_width_in_cm": "mean",
                                                 "json.petal_length_in_cm": "mean",
                                                 "json.petal_width_in_cm": "mean"})


print("DataFrame is ")

df3.writeStream \
    .format("console") \
    .option("truncate","false") \
    .start() \
    .awaitTermination()

