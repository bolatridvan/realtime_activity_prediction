import os

#Directories

baseDir = '/root/realtime_activity_prediction/'
datasetDir = os.path.join(baseDir, 'dataset/')
rawDatasetDir = os.path.join(datasetDir, 'KETI/')
parquetDatasetDir = os.path.join(datasetDir, 'parquet_files/')
scriptsDir = os.path.join(baseDir, 'scripts/')
modelDir = os.path.join(baseDir, 'model/')
#Model

trainRate = 0.8
testRate = 0.2
inputColumns = ['co2','humidity','light','temperature']

#Spark

sparkPath = '/opt/spark'

defaultSparkConfig = {
    "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.executor.memory": "8g",
    "spark.driver.memory": "8g"
}

#Kafka

checkpoint_dir = "file:///tmp/streaming/realtime_activity_prediction_postgres"
kafkaHost = 'kafka'
kafkaPort = '9092'

#Postgresql

postgresHost = 'postgresql'
postgresPort = '5432'
postgresDefaultDb = 'sensors'
postgresDefaultTable = 'occupancy'
jdbcurl = 'jdbc:postgresql://'+postgresHost+':'+postgresPort+'/'+postgresDefaultDb
postgresqlUser = 'train'
postgresqlPass = 'Ankara06'

#print(baseDir)
#print(datasetDir)
#print(rawDatasetDir)
#print(parquetDatasetDir)
#print(modelDir)
