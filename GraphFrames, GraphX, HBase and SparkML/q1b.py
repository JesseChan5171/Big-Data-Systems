from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from graphframes import *
import pyspark.sql.functions as f

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

actions = sqlContext.read.csv("hdfs:///user/s1155124983/hw5_q1/mooc_actions.tsv", header = True, sep ='\t')\
.withColumnRenamed("USERID", "src").withColumnRenamed("TARGETID", "dst")

mocc_vertices = sqlContext.read.csv("hdfs:///user/s1155124983/hw5_q1/vertices.tsv", header = True, sep ='\t')

mocc_g = GraphFrame(mocc_vertices, actions)

mocc_g_fil = mocc_g.filterEdges("TIMESTAMP >= 10000 and TIMESTAMP <= 50000").dropIsolatedVertices()

num_v = mocc_g_fil.vertices.count()
print(num_v)

num_e = mocc_g_fil.edges.count()
print(num_e)    


