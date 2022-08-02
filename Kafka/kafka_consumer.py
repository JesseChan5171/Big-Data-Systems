from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import json
import time

def process_rdd(time, rdd):
  print("---------- %s -----------" % str(time))
  try:
    print(rdd.top(30, key=lambda x: x[1]))
  except:
    e = sys.exc_info()
    print("Error: ", e)

if __name__ == '__main__':
  sc = SparkContext(appName="test")
  sc.setLogLevel("WARN")

  ssc = StreamingContext(sc, 10)
  ssc.checkpoint('./checkpoint')
  kafkaStream = KafkaUtils.createStream(ssc, 'dicvmd7.ie.cuhk.edu.hk:2181', 'test', {'983-test': 1})

  words = kafkaStream.flatMap(lambda x: x[1].split()).filter(lambda x: x.startswith("#") and x[1:].lower() != "bitcoin" and len(x) > 1).map(lambda x: (x,1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 300, 120)
  words.foreachRDD(process_rdd)
  
  ssc.start()
  ssc.awaitTermination()
