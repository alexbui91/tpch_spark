from pyspark.sql import SparkSession
from pyspark import SparkConf
# from pyspark.sql import SQLContext
from query import *
import time
import preload
import argparse


spark, sc = None, None
prefix = ''
tables = ['customer','lineitem','nation','region','part','partsupp','orders','supplier']
all_parquet = 1

def init_spark(pre=0, ap=1):
    global spark, sc, prefix, tables
    # spark = SparkSession.builder.appName("assignment").getOrCreate()
    conf = (SparkConf().setAppName("assignment"))
    # conf.set("spark.driver.memory", "32g")
    # conf.set("spark.executor.memory", "16g")
    # conf.set("spark.ui.port", "31040")
    # conf.set("spark.sql.shuffle.partitions", "100")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    if pre:
        preload.init_spark(spark)
    elif ap:
        print(1)
        preload.load_parquet(spark, prefix, tables)


def execute_all():
    for n, q in queries.iteritems():
        try:
            execute(n, q)
        except Exception as e: 
            print('Error occured', e)
            continue


def execute(qname, query):
    global spark, prefix
    # load balancing each table
    tables = []
    if not all_parquet:
        tables = tables_need[qname]
        preload.load_parquet(spark, prefix, tables)
        print(tables)
    start = time.time()
    result = spark.sql(query)
    result.show()
    duration = time.time() - start
    print("Time to run query %s is: %is" % (qname, duration))
    if not all_parquet:
        preload.remove_parquet(spark, tables)


queries = {'q1': q1, 'q3': q3, 'q5': q5, 'q7': q7, 'q16': q16, 'q18': q18, 'q20': q20, 'q22': q22}
tables_need = {
    'q1': ['lineitem'], 
    'q3': ['customer', 'orders', 'lineitem'], 
    'q5': ['customer', 'orders', 'lineitem', 'supplier', 'nation', 'region'], 
    'q7': ['supplier', 'lineitem', 'orders', 'customer', 'nation'], 
    'q16': ['PARTSUPP', 'PART', 'SUPPLIER'], 
    'q18': ['customer', 'orders', 'lineitem'], 
    'q20': ['supplier', 'nation', 'partsupp', 'part', 'lineitem'], 
    'q22': ['customer', 'orders']
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--preload", default=0, type=int)
    parser.add_argument("-pt", "--parquet", default=1, type=int)
    parser.add_argument("-pf", "--prefix")
    parser.add_argument("-q", "--query")
    parser.add_argument("-a", "--all", default=0, type=int)
    
    args = parser.parse_args()
    if not args.prefix:
        prefix = ''
    else: 
        prefix = args.prefix
    all_parquet = args.parquet
    init_spark(args.preload, args.parquet)
    if args.all:
        execute_all()
    elif args.query and args.query in queries:
        execute(args.query, queries[args.query])