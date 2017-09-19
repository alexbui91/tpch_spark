from pyspark.sql.types import *

s_customer = StructType([
    StructField("C_CUSTKEY", IntegerType(), False),
    StructField("C_NAME", StringType(), True),
    StructField("C_ADDRESS", StringType(), True),
    StructField("C_NATIONKEY", IntegerType(), False),
    StructField("C_PHONE", StringType(), True),
    StructField("C_ACCTBAL", DoubleType(), True),
    StructField("C_MKTSEGMENT", StringType(), True),
    StructField("C_COMMENT", StringType(), True)])

s_lineitem = StructType([
    StructField("L_ORDERKEY", IntegerType(), False),
    StructField("L_PARTKEY", IntegerType(), False),
    StructField("L_SUPPKEY", IntegerType(), False),
    StructField("L_LINENUMBER", IntegerType(), True),
    StructField("L_QUANTITY", DoubleType(), True),
    StructField("L_EXTENDEDPRICE", DoubleType(), True),
    StructField("L_DISCOUNT", DoubleType(), True),
    StructField("L_TAX", DoubleType(), True),
    StructField("L_RETURNFLAG", StringType(), True),
    StructField("L_LINESTATUS", StringType(), True),
    StructField("L_SHIPDATE", DateType(), True),
    StructField("L_COMMITDATE", DateType(), True),
    StructField("L_RECEIPTDATE", DateType(), True),
    StructField("L_SHIPINSTRUCT", StringType(), True),
    StructField("L_SHIPMODE", StringType(), True),
    StructField("L_COMMENT", StringType(), True)])

s_orders = StructType([
    StructField("O_ORDERKEY", IntegerType(), False),
    StructField("O_CUSTKEY", IntegerType(), False),
    StructField("O_ORDERSTATUS", StringType(), True),
    StructField("O_TOTALPRICE", DoubleType(), True),
    StructField("O_ORDERDATE", DateType(), True),
    StructField("O_ORDERPRIORITY", StringType(), True),
    StructField("O_CLERK", StringType(), True),
    StructField("O_SHIPPRIORITY", StringType(), True),
    StructField("O_COMMENT", StringType(), True)])

s_nation = StructType([
    StructField("N_NATIONKEY", IntegerType(), False),
    StructField("N_NAME", StringType(), True),
    StructField("N_REGIONKEY", IntegerType(), False),
    StructField("N_COMMENT", StringType(), True)])

s_region = StructType([
    StructField("R_REGIONKEY", IntegerType(), False),
    StructField("R_NAME", StringType(), True),
    StructField("R_COMMENT", StringType(), True)])

s_part = StructType([
    StructField("P_PARTKEY", IntegerType(), False),
    StructField("P_NAME", StringType(), True),
    StructField("P_MFGR", StringType(), True),
    StructField("P_BRAND", StringType(), True),
    StructField("P_TYPE", StringType(), True),
    StructField("P_SIZE", StringType(), True),
    StructField("P_CONTAINER", StringType(), True),
    StructField("P_RETAILPRICE", DoubleType(), True),
    StructField("P_COMMENT", StringType(), True)])

s_supplier = StructType([
    StructField("S_SUPPKEY", IntegerType(), False),
    StructField("S_NAME", StringType(), True),
    StructField("S_ADDRESS", StringType(), True),
    StructField("S_NATIONKEY", IntegerType(), False),
    StructField("S_PHONE", StringType(), True),
    StructField("S_ACCTBAL", DoubleType(), True),
    StructField("S_COMMENT", StringType(), True)])

s_partsupp = StructType([
    StructField("PS_PARTKEY", IntegerType(), False),
    StructField("PS_SUPPKEY", IntegerType(), False),
    StructField("PS_AVAILQTY", StringType(), True),
    StructField("PS_SUPPLYCOST", DoubleType(), True),
    StructField("PS_COMMENT", StringType(), True)])
