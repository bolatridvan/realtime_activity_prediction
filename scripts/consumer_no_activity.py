import conf
import findspark
findspark.init(conf.sparkPath)
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.types import FloatType

spark = SparkSession.builder \
.appName("Spark Test") \
.config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.config("spark.executor.memory", "10g") \
.config("spark.driver.memory", "10g") \
.master("local[8]") \
.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

assembler = VectorAssembler(inputCols=[
    'co2',
    'humidity',
    'light',
    'temperature'
],
                           outputCol='features')


df = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "kafka:9092")
.option("subscribe", "activity_test_data")
.load())


df2 = df.selectExpr("CAST(value AS STRING)")

df3 = df2.withColumn("Timestamp", F.trim(F.split(F.col("value"), ",")[0])) \
         .withColumn("room_id", F.trim(F.split(F.col("value"), ",")[6]))  \
         .withColumn("co2", F.trim(F.split(F.col("value"), ",")[1]).cast(FloatType())) \
         .withColumn("humidity", F.trim(F.split(F.col("value"), ",")[2]).cast(FloatType())) \
         .withColumn("light", F.trim(F.split(F.col("value"), ",")[3]).cast(FloatType())) \
         .withColumn("temperature", F.trim(F.split(F.col("value"), ",")[5]).cast(FloatType())) \
         .withColumn("pir_class", F.trim(F.split(F.col("value"), ",")[7]).cast(FloatType()))

df4 = assembler.transform(df3)

model = RandomForestClassificationModel.load(conf.modelDir + "rf.model")

predictions = model.transform(df4) \
                    .withColumn('Activity', F.udf(lambda x: 'occupied' if x==1 else 'not occupied')('prediction')) \
                    .withColumn('topic', F.udf(lambda x: 'activity' if x==1 else 'noactivity')('pir_class')) \
                    .drop(*['value', 'features', 'rawPrediction', 'probability']) \
                    .filter('prediction == pir_class') \
                    .withColumn('value', F.concat_ws(',','Timestamp', 'room_id', 'co2', 'humidity', 'light', 'temperature', 'Activity')) \
                    .selectExpr("CAST(value AS STRING)", "pir_class", "prediction")

df_noactivity = predictions.filter("prediction == 0") #.drop('pir_class')

checkpoint_dir = "file:///tmp/streaming/realtime_activity_prediction_noactivity"

noactivity = (df_noactivity
.writeStream
.format("kafka")
.outputMode("append")
.trigger(processingTime="0.1 second")
.option("checkpointLocation", checkpoint_dir)
.option("kafka.bootstrap.servers", "kafka:9092")
.option("topic", 'noactivity')
.start())


# start streaming
noactivity.awaitTermination()
