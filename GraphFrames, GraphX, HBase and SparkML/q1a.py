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

# mocc_g.inDegrees.show()
# mocc_g.vertices.show()

# Num. of Ver
num_v = mocc_g.vertices.count()
print(num_v)

# Num. of Users
num_u = mocc_g.vertices.filter("type = 'User'").count()
print(num_u)

# Num. of Course Activity
num_ca = mocc_g.vertices.filter("type = 'Course Activity'").count()
print(num_ca)

# the number of edges
num_e = mocc_g.edges.count()
print(num_e)
    
# the vertex with the largest in-degree
mocc_g.inDegrees.orderBy(f.desc("inDegree")).limit(1).show()

# the vertex with the largest out-degree
mocc_g.outDegrees.orderBy(f.desc("outDegree")).limit(1).show()








