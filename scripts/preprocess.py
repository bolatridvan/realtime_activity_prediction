import conf
import findspark
findspark.init(conf.sparkPath)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import os
import sys

def get_spark(config_map):
    spark = SparkSession.builder \
            .config(map=config_map) \
            .master("local[8]") \
            .getOrCreate()
    return spark
    
def get_emptyDF(spark: SparkSession, columns: StructType):
    emp_RDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(data = emp_RDD, schema = columns)
    return df

def write_to_parquet(spark: SparkSession):

    for dirname, _, filenames in os.walk(conf.rawDatasetDir):

        if len(filenames) > 1:
            room_df = get_emptyDF(spark = spark, columns = StructType([]))
            room_id = dirname.split('/')[-1]

            for i, filename in enumerate(filenames):

                feat_name = filename.split('.')[0]

                schema = StructType([
                    StructField("TimeStamp",IntegerType(),True),
                    StructField(feat_name,FloatType(),True)
                                    ]) 
                tmp_df = spark.read.format("csv").schema(schema).load(os.path.join(dirname, filename))

                if i == 0:
                    room_df = tmp_df
                    
                else:
                    room_df = room_df.join(tmp_df, ['TimeStamp'], how='full')
        
            room_df = room_df.withColumn("room_ID", F.lit(room_id))
            room_df.write.format('parquet') \
                    .mode("overwrite") \
                    .save(conf.parquetDatasetDir + room_id + '.parquet')
            print('{}.parquet is saved.'.format(room_id))

if __name__ == "__main__":

    spark = get_spark(config_map = conf.defaultSparkConfig)

    df = get_emptyDF(spark=spark, columns=StructType([
                                StructField("TimeStamp",IntegerType(),True),
                                StructField("co2",FloatType(),True),
                                StructField("humidity",FloatType(),True),
                                StructField("light",FloatType(),True),
                                StructField("pir",FloatType(),True),
                                StructField("temperature",FloatType(),True),
                                StructField("room_ID",StringType(),True)
                                                ]))
    
    write_to_parquet(spark=spark)
    spark.stop()
   


        

