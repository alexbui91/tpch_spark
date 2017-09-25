from subprocess import Popen,PIPE,STDOUT,call
from pyspark.sql import SparkSession
from pyspark import SparkConf
# from pyspark.sql import SQLContext
from query import *
import time
import preload
import argparse
import properties as pr


spark, sc = None, None
prefix = ''
tables = ['customer','lineitem','nation','region','part','partsupp','orders','supplier']
all_parquet = 1

def init_spark(pre=0, ap=1):
    global spark, sc, prefix, tables
    # spark = SparkSession.builder.appName("assignment").getOrCreate()
    conf = (SparkConf().setAppName("assignment"))
    conf.set("spark.driver.memory", pr.dmem)
    conf.set("spark.executor.memory", pr.emem)
    conf.set("spark.ui.port", pr.port)
    # conf.set("spark.sql.shuffle.partitions", "100")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    if pre:
        preload.init_spark(spark)
    elif ap:
        preload.load_parquet(spark, prefix, tables)


def execute_all(cache):
    for n, q in queries.iteritems():
        try:
            execute(n, q, cache)
        except Exception as e: 
            print('Error occured', e)
            continue


def execute(qname, query, cache):
    global spark, prefix
    # load balancing each table
    tables = []
    start = time.time()
    get_memory(qname)
    if not all_parquet:
        tables = pr.tables_need[qname]
        if cache: 
            preload.cache(spark, tables, prefix)
        else:
            preload.load_parquet(spark, prefix, tables)
    get_memory(qname)
    after = time.time()
    loading_time = after - start
    result = spark.sql(query)
    get_memory(qname)
    result.show()
    get_memory(qname)
    end = time.time()
    running_time = end - after
    duration = end - start
    print("Time to load query %s is: %is" % (qname, loading_time))
    print("Time to run query %s is: %is" % (qname, running_time))
    print("Total time of query %s is: %is" % (qname, duration))
    get_memory(qname)
    print_time(qname, loading_time, running_time, duration)


def print_time(qname, loading_time, running_time, duration):
    global prefix
    now = time.time()
    output = ("Time to load query %s is: %is" % (qname, loading_time)) + '\n' \
        ("Time to run query %s is: %is" % (qname, running_time)) + '\n' \
        ("Total time of query %s is: %is" % (qname, duration))
    with open("%s_time_%s.txt" % (prefix, qname), 'a') as file:
        file.write(('%f' % now) + '\n' + output)


def get_memory(name):
    now = time.time()
    proc=Popen('free -m', shell=True, stdout=PIPE, )
    output=proc.communicate()[0]
    with open("%s.txt" % name, 'a') as file:
        file.write(('%f' % now) + '\n' + output)


queries = {'q1': q1, 'q3': q3, 'q5': q5, 'q7': q7, 'q16': q16, 'q18': q18, 'q20': q20, 'q22': q22}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--preload", default=0, type=int)
    parser.add_argument("-pt", "--parquet", default=1, type=int)
    parser.add_argument("-pf", "--prefix")
    parser.add_argument("-q", "--query")
    parser.add_argument("-a", "--all", default=0, type=int)
    parser.add_argument("-c", "--cache", default=0, type=int)

    args = parser.parse_args()
    if not args.prefix:
        prefix = ''
    else: 
        prefix = args.prefix
    all_parquet = args.parquet
    init_spark(args.preload, args.parquet)
    if args.all:
        execute_all(args.cache)
    elif args.query and args.query in queries:
        execute(args.query, queries[args.query], args.cache)    

        