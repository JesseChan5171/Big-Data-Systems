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


# i
print("i\n")
path_i = mocc_g_fil.find("(a)-[e1]->(b); (c) - [e2] -> (b)")\
  .filter("a.id != c.id")\
  .filter("e1.timestamp <= e2.timestamp")
  
path_i.show()

count_i = path_i.count()
print(count_i)

# ii
print("ii\n")
path_ii = mocc_g_fil.find("(a)-[e1]->(b); (b) - [e2] -> (c)")\
  .filter("a.id != b.id and b.id != c.id")\
  .filter("e1.timestamp <= e2.timestamp")
  
path_ii.show()

count_ii = path_ii.count()
print(count_ii)

# iii
print("iii\n")
path_iii = mocc_g_fil.find("(a)-[e1]->(c); (b) - [e4] -> (c); (a) - [e3] -> (d); (b) - [e2] -> (d)")\
  .filter("a.id != b.id and c.id != d.id")\
  .filter("e1.timestamp <= e2.timestamp and e2.timestamp <= e3.timestamp and e3.timestamp <= e4.timestamp")
  
path_iii.show()

path_iii = path_iii.count()
print(path_iii)

# iv
print("iv\n")
path_iv = mocc_g_fil.find("(d)-[e1]->(a); (b) - [e3] -> (c); (d) - [e4] -> (c); (d) - [e2] -> (e)")\
  .filter("a.id != c.id and b.id != d.id and c.id != e.id and a.id != e.id")\
  .filter("e1.timestamp <= e2.timestamp and e2.timestamp <= e3.timestamp and e3.timestamp <= e4.timestamp")
  
path_iv.show()

path_iv = path_iv.count()
print(path_iv)