import conf
import findspark
findspark.init(conf.sparkPath)
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.types import FloatType
from preprocess import get_spark

spark = get_spark(conf.defaultSparkConfig)
spark.sparkContext.setLogLevel('ERROR')
model = RandomForestClassificationModel.load(conf.modelDir + "rf.model")

assembler = VectorAssembler(inputCols = conf.inputColumns,
                            outputCol='features')


df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "activity_test_data")
        .load())

df2 = df.selectExpr("CAST(value AS STRING)") \
         .withColumn("Timestamp", F.trim(F.split(F.col("value"), ",")[0])) \
         .withColumn("roomId", F.trim(F.split(F.col("value"), ",")[6]))  \
         .withColumn("co2", F.trim(F.split(F.col("value"), ",")[1]).cast(FloatType())) \
         .withColumn("humidity", F.trim(F.split(F.col("value"), ",")[2]).cast(FloatType())) \
         .withColumn("light", F.trim(F.split(F.col("value"), ",")[3]).cast(FloatType())) \
         .withColumn("temperature", F.trim(F.split(F.col("value"), ",")[5]).cast(FloatType())) \
         .withColumn("pir_class", F.trim(F.split(F.col("value"), ",")[7]).cast(FloatType()))

df3 = assembler.transform(df2)

predictions = model.transform(df3) \
                    .withColumn('activity', F.udf(lambda x: 'occupied' if x==1 else 'not occupied')('prediction')) \
                    .drop(*['value', 'features', 'rawPrediction', 'probability']) \
                    .selectExpr("from_unixtime(Timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp", "roomId", "co2", "humidity", "light", "temperature", "activity")

def _write_postgres(df, epoch_id) -> None:

    options = {
        'driver': 'org.postgresql.Driver',
        'url': conf.jdbcurl,
        'dbtable': conf.postgresDefaultTable,
        'user': conf.postgresqlUser,
        'password': conf.postgresqlPass
    }

    df.write \
        .mode('append') \
        .format('jdbc') \
        .options(**options) \
        .save()

predictions.writeStream \
        .foreachBatch(_write_postgres) \
        .option('checkpointLocation', conf.checkpoint_dir) \
        .start() \
        .awaitTermination()


# start streaming
#activity.awaitTermination()



