from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.preprocessing import LabelEncoder
from subprocess import run,call,Popen


json_string = 'ghghghgh'
def createSparkSession():
    spark = SparkSession \
        .builder \
        .appName("learning_01") \
        .master("local") \
        .getOrCreate()

    return spark

def createInitialDataFrame(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "irisDataSetTopic") \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def inputSchema():
    schema = StructType([
        StructField("sepal_length_in_cm", DoubleType()), \
        StructField("sepal_width_in_cm", DoubleType()), \
        StructField("petal_length_in_cm", DoubleType()), \
        StructField("petal_width_in_cm", DoubleType()), \
        StructField("class", StringType()), \
        StructField("emni", StringType())
    ])
    return schema

def outputSchema():
    output_schema = StructType([
    #StructField("class", StringType()),
    StructField("mean_sepal_length", DoubleType()),
    StructField("mean_sepal_width", DoubleType()),
    StructField("mean_petal_length", DoubleType()),
    StructField("mean_petal_width", DoubleType()),
    ])

    return output_schema

def outputOfScikitLearnSchema():
    output_of_scikit_learn = StructType([
        StructField("c1", DoubleType()),
        StructField("c2", DoubleType()),
    ])

    return output_of_scikit_learn

def outputAsJson(pd_df):
    json_string = pd_df.to_json(path_or_buf='test2.json' ,orient= 'split')
    print(json_string)

output_of_scikit_learn = outputOfScikitLearnSchema()
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
    sklearn_lda = LDA()
    X_lda_sklearn = sklearn_lda.fit_transform(X,y)
    X_lda_sklearn_df = pd.DataFrame(X_lda_sklearn)
    outputAsJson(X_lda_sklearn_df)
    #self.json_string = X_lda_sklearn_df.to_json()
    return X_lda_sklearn_df
    #result.reset_index(inplace=True, drop=False)
    #return pd.DataFrame(X_lda_sklearn)

def writeStream(df3):
    df3.writeStream \
        .format("console") \
        .option("truncate","false") \
        .start() \
        .awaitTermination()
def runSparkShellCommand():
    process = Popen(["sh", "/Users/khanhafizurrahman/Desktop/Thesis/code/Thesis_Implementation/terminal_commands/spark_start.sh"])
    #process.wait()

def kafkaAnalysisProcess():
    spark = createSparkSession()
    df = createInitialDataFrame(spark)
    df = df.selectExpr("CAST(value AS STRING)")
    schema = inputSchema()
    df1 = df.select(from_json(df.value, schema).alias("json"))
    df2 = df1.select('json.*')
    df3 = df2.groupby("emni").apply(g)
    return df3


if __name__ == '__main__':
    #runSparkShellCommand()
    kafkaAnalysisProcess()
    write_to_kafka_df = kafkaAnalysisProcess()

    columns_of_the_schema = write_to_kafka_df.columns
    #X5 = pd.DataFrame(write_to_kafka_df['c1'])
    #print(type(X5))
    for column in columns_of_the_schema:
        pass

    #output_df = pd.DataFrame(write_to_kafka_df['c1'])
    #print(output_df.head())
    writeStream(df3= write_to_kafka_df)

"""def calculate_mean(x):
    return x.loc[:, ["sepal_length_in_cm", "sepal_width_in_cm", "petal_length_in_cm", "petal_width_in_cm"]].mean()

mean_val_calc = df1.groupBy('json.class').agg({"json.sepal_length_in_cm": "mean" ,
                                                 "json.sepal_width_in_cm": "mean",
                                                 "json.petal_length_in_cm": "mean",
                                                 "json.petal_width_in_cm": "mean"})

print("DataFrame is ")"""






