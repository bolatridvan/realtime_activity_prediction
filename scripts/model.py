import conf
import findspark
findspark.init(conf.sparkPath)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
import os
import sys
from preprocess import get_spark, get_emptyDF

spark = get_spark(conf.defaultSparkConfig)

df = get_emptyDF(spark=spark, columns=StructType([
                                StructField("TimeStamp",IntegerType(),True),
                                StructField("co2",FloatType(),True),
                                StructField("humidity",FloatType(),True),
                                StructField("light",FloatType(),True),
                                StructField("pir",FloatType(),True),
                                StructField("temperature",FloatType(),True),
                                StructField("room_ID",StringType(),True)
                                                ]))

for pq in os.listdir(conf.parquetDatasetDir):
    room_df = spark.read.format("parquet").option('header', True).load(conf.parquetDatasetDir + pq)
    df = df.union(room_df)
    print(pq.split('.')[0], ' successfully extracted.')

@F.udf(IntegerType())
def pir_to_class(pir):
    if pir == None:
        return 2
    elif pir == 0:
        return 0
    elif pir > 0:
        return 1
    
df2 = df.withColumn("pir_class", pir_to_class(F.col("pir")))
df_zeros = df2.filter("pir_class == 0")
df_ones = df2.filter("pir_class == 1")
df_twos = df2.filter("pir_class == 2")

zeros_train, zeros_test = df_zeros.randomSplit([conf.trainRate, conf.testRate], seed=13) 
ones_train, ones_test = df_ones.randomSplit([conf.trainRate, conf.testRate], seed=13) 
twos_train, twos_test = df_twos.randomSplit([conf.trainRate, conf.testRate], seed=13) 

train_df = zeros_train.dropna().union(ones_train.dropna()).union(twos_train.dropna())
test_df = zeros_test.union(ones_test)

print('Saving test data..')

pandasDF = test_df.toPandas()
test_df = test_df.sort(['TimeStamp'])
pandasDF.to_csv(conf.modelDir + 'test_data.csv', header = True, index = False, encoding='utf-8')

assembler = VectorAssembler(inputCols=[
    'co2',
    'humidity',
    'light',
    'temperature'
],
                           outputCol='features')

train_model = assembler.transform(train_df)
train_model = train_model.select(['features', 'pir_class']).withColumnRenamed("pir_class","label")

print('Caching dataset..')

train_model.persist()

print('Training model..')

rf = RandomForestClassifier().fit(train_model)

print('Saving model..')

rf.write().overwrite().save(conf.modelDir + "rf.model")

spark.stop()