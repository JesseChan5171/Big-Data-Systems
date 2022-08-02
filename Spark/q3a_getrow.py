from pyspark.sql import SparkSession
from pyspark.context import SparkContext

sc = SparkSession.builder.appName("q3a").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g").config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()

df = sc.read.csv('hdfs:///user/s1155124983/Crime_Incidents_in_2013.csv',header=True)
df_drop_empCol = df.drop("OCTO_RECORD_ID")
df_drop_cmpCol = df_drop_empCol.na.drop(how = 'any')
df_tarCol = df_drop_empCol.select("CCN", "REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT")

df_tarCol.write.csv('hdfs:/user/s1155124983/hw3')

