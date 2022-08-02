from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from  pyspark.sql.functions import input_file_name

sc = SparkSession.builder.appName("q3a").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g").config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

path = 'hdfs:///user/s1155124983/hw3_crime1018/*.csv'

#df = sc.read.csv('hdfs:///user/s1155124983/Crime_Incidents_in_2013.csv',header=True)
df = spark.read.format("csv") \
   .option("header", "true") \
   .load(path) \
   .withColumn("filename", input_file_name())

df_drop_empCol = df.drop("OCTO_RECORD_ID")
df_drop_cmpCol = df_drop_empCol.na.drop(how = 'any')
df_tarCol = df_drop_empCol.select("REPORT_DAT", "METHOD")
#df_tarCol.write.csv('hdfs:/user/s1155124983/hw3')

#df_gpbyoffcount = df_tarCol.groupBy("OFFENSE").count().orderBy("count", ascending = False)

from pyspark.sql.functions import split
from pyspark.sql.functions import col
#df_Time = df_tarCol.select("REPORT_DAT").withColumn("Re_Time", split(split(col("REPORT_DAT")," ").getItem(1), ":").getItem(0))
#df_time_count = df_Time.groupBy("Re_Time").count().orderBy("count", ascending = False)
df_gun = df_tarCol.filter(col("METHOD") == "GUN")

df_year = df_gun.withColumn("Year", split(col("REPORT_DAT"),"/").getItem(0))
df_ycount = df_gpbyyear = df_year.groupBy("Year").count().orderBy("Year")


