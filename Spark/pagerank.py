
import re
import sys
from operator import add
from pyspark.sql import SparkSession
import pyspark

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def split_line(line):
    parts = re.split(r'\s+', line)
    return parts[0], parts[1]


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: split_line(urls)).distinct().groupByKey()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    for iteration in range(int(sys.argv[2])):
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]
        ))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    top100 = ranks.top(100, key=lambda x: x[1])
    print(top100)
    spark.stop()

