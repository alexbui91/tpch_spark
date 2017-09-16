from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext
from structure import *
from query import *

spark = SparkSession.builder.appName("assignment").getOrCreate()
sc = spark.sparkContext

# customer = sc.textFile("/home/alex/Documents/tpch/dbgen/customer.tbl") \
#             .map(lambda row: row.split('|'))

# lineitem = sc.textFile("/home/alex/Documents/tpch/dbgen/lineitem.tbl") \
#             .map(lambda row: row.split('|')) \
#             .map(lambda row: [int(row[0]), int(row[1]), int(row[2]), 
#                                 int(row[3]), float(row[4]), float(row[5]), 
#                                 float(row[6]), float(row[7])] + row[8:-1])

# nation = sc.textFile("/home/alex/Documents/tpch/dbgen/nation.tbl") \
#             .map(lambda row: row.split('|')) \
#             .map(lambda row: [int(row[0]), row[1], int(row[2]), row[3]])

region = sc.textFile("/home/alex/Documents/tpch/dbgen/region.tbl") \
            .map(lambda row: row.split('|')) \
            .map(lambda row: [int(row[0]), row[1], row[2]])

# df = spark.createDataFrame(customer, s_customer)
# df.registerTempTable('customer')

# df = spark.createDataFrame(lineitem, s_lineitem)
# df.registerTempTable('lineitem')

# df = spark.createDataFrame(nation, s_nation)
# df.registerTempTable('nation')

df = spark.createDataFrame(region, s_region)
df.registerTempTable('region')

# re_q1 = spark.sql(q1)
# re_q1.show()