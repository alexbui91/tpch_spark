from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext
from structure import *
from query import *
from datetime import datetime
import properties as p


spark, sc = None, None
schema_entities = {}
tables = ['customer','lineitem','nation','region','part','partsupp','orders','supplier']


def init_table(baseurl):
    date_format = '%Y-%m-%d'
    customer = sc.textFile("%s/customer.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(row[0])] + row[1:3] + [int(row[3])] + [row[4]] + [float(row[5])] + row[6:-1])

    lineitem = sc.textFile("%s/lineitem.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(x) for x in row[0:4]] + [float(x) for x in row[4:8]] + row[8:10] + [datetime.strptime(x, date_format) for x in row[10:13]] + row[13:-1])

    nation = sc.textFile("%s/nation.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(row[0]), row[1], int(row[2]), row[3]])

    region = sc.textFile("%s/region.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(row[0]), row[1], row[2]])


    part = sc.textFile("%s/part.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(row[0])] + row[1:5] + [int(row[5]), row[6], float(row[7]), row[8]])

    partsupp = sc.textFile("%s/partsupp.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(x) for x in row[0:3]] + [float(row[3])] + [row[4]])

    orders = sc.textFile("%s/orders.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(x) for x in row[0:2]] + [row[2]] + [float(row[3])] + [datetime.strptime(row[4], date_format)] + row[5:7] + [int(row[7])] + [row[8]])

    supplier = sc.textFile("%s/supplier.tbl" % baseurl) \
                .map(lambda row: row.split('|')) \
                .map(lambda row: [int(row[0])] + row[1:3] + [int(row[3])] + [row[4]] + [float(row[5])] + [row[6]])
    return customer, lineitem, nation, region, region, part, partsupp, orders, supplier


def init_spark(sp=None):
    global spark, sc, schema_entities
    if sp:
        spark = sp
    else:
        spark = SparkSession.builder.appName("assignment").getOrCreate()
    sc = spark.sparkContext
    customer, lineitem, nation, region, region, part, partsupp, orders, supplier = init_table(p.baseurl)
    schema_entities = {
        'customer': (customer, s_customer), 
        'lineitem': (lineitem, s_lineitem), 
        'nation': (nation, s_nation), 
        'region': (region, s_region), 
        'part': (part, s_part), 
        'partsupp': (partsupp, s_partsupp), 
        'orders': (orders, s_orders), 
        'supplier': (supplier, s_supplier)
    }


def init_temple_dataframe():
    for key, value in schema_entities.iteritems():
        df = spark.createDataFrame(value[0], value[1])
        df.registerTempTable(key)
        # check dataframe
        # print(df.first())


def create_parquet(spark):
    for key, value in schema_entities.iteritems():
        df = spark.createDataFrame(value[0], value[1])
        # check dataframe
        # print(df.first())
        # save dataframe to parquet to save time
        df.write.format("parquet").save("%s.parquet" % key, mode='overwrite')


def load_parquet(spark):
    for x in tables:
        df = spark.read.load("%s.parquet" % x)
        df.registerTempTable(x)


# df = spark.createDataFrame(customer, s_customer)
# df.registerTempTable('customer')
# print(df.first())
if __name__ == '__main__':
    init_spark()
    create_parquet(spark)
    # init temp table
    