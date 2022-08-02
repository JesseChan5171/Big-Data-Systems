from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as F
from pyspark.sql.functions import window
from pyspark.sql.functions import size

            
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Q3_t") \
        .getOrCreate()
        
    lines = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "dicvmd7.ie.cuhk.edu.hk:6667") \
      .option("subscribe", '983-ft') \
      .load()
      
    # print(lines.isStreaming())
    # lines.isStreaming()
    
    words = lines.select(
       explode(
        split(lines.value, " ")
       ).alias("word"), "timestamp"
    )
   
    
    wordCounts = words.select("*")\
    .filter(words.word.startswith("#")) \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window("timestamp", "10 minutes", "5 minutes"),
        "word") \
    .count().sort(F.col("count").desc())
    
    
    query = wordCounts \
    .writeStream \
    .option("checkpointLocation", "./checkpoint") \
    .outputMode("complete") \
    .format("console") \
    .start()
    
   
    query.awaitTermination()


