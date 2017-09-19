from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext
from query import *
import time
import preload
import argparse

spark, sc = None, None

def init_spark(pre=False):
    global spark, sc
    spark = SparkSession.builder.appName("assignment").getOrCreate()
    sc = spark.sparkContext
    if pre:
        preload.init_spark(spark)
    else:
        preload.load_parquet(spark)


def execute_all():
    for n, q in queries.iteritems():
        try:
            execute(n, q)
        except Exception as e: 
            print('Error occured', e)
            continue


def execute(qname, query):
    global spark
    start = time.time()
    result = spark.sql(query)
    result.show()
    duration = time.time() - start
    print("Time to run query %s is: %ims" % (qname, duration))


queries = {'q1': q1, 'q3': q3, 'q5': q5, 'q7': q7, 'q16': q16, 'q18': q18, 'q20': q20, 'q22': q22}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--preload", default=False, type=bool)
    parser.add_argument("-q", "--query")
    parser.add_argument("-a", "--all", default=False, type=bool)
    args = parser.parse_args()
    init_spark(args.preload)
    if args.all:
        execute_all()
    elif args.query and args.query in queries:
        execute(args.query, queries[args.query])