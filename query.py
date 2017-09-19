q1 = "select L_RETURNFLAG, L_LINESTATUS, \
    sum(L_QUANTITY) as sum_qty, \
    sum(L_EXTENDEDPRICE) as sum_base_price, \
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as sum_disC_PRICE, \
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as sum_charge, \
    avg(L_QUANTITY) as avg_qty, \
    avg(L_EXTENDEDPRICE) as avg_price, \
    avg(L_DISCOUNT) as avg_disc, \
    count(*) as count_order \
    from lineitem \
    where L_SHIPDATE <= '1998-09-16' \
    group by L_RETURNFLAG, L_LINESTATUS \
    order by L_RETURNFLAG, L_LINESTATUS"

q3 = "select L_ORDERKEY, \
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as REVENUE, \
    O_ORDERDATE, \
    O_SHIPPRIORITY \
    from CUSTOMER, ORDERS, LINEITEM \
    where C_MKTSEGMENT = 'BUILDING' \
        and C_CUSTKEY = O_CUSTKEY \
        and L_ORDERKEY = O_ORDERKEY \
        and O_ORDERDATE < '1995-03-22' \
        and L_SHIPDATE > '1995-03-22' \
    group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY \
    order by REVENUE desc, O_ORDERDATE \
    limit 10"

q5 = "select N_NAME, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE \
    from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION \
    where C_CUSTKEY = O_CUSTKEY \
        AND L_ORDERKEY = O_ORDERKEY \
        AND L_SUPPKEY = S_SUPPKEY \
        AND C_NATIONKEY = S_NATIONKEY \
        AND S_NATIONKEY = N_NATIONKEY \
        AND N_REGIONKEY = R_REGIONKEY \
        AND R_NAME = 'AFRICA' \
        AND O_ORDERDATE >= '1993-01-01' \
        AND O_ORDERDATE < '1994-01-01' \
    group by N_NAME \
    order by REVENUE DESC"

q7 = "select SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE \
    from ( SELECT \
                N1.N_NAME AS SUPP_NATION, \
                N2.N_NAME AS CUST_NATION, \
                YEAR(L_SHIPDATE) AS L_YEAR, \
                L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME \
            FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2 \
            WHERE S_SUPPKEY = L_SUPPKEY \
                AND O_ORDERKEY = L_ORDERKEY \
                AND C_CUSTKEY = O_CUSTKEY \
                AND S_NATIONKEY = N1.N_NATIONKEY \
                AND C_NATIONKEY = N2.N_NATIONKEY \
                AND ((N1.N_NAME = 'KENYA' AND N2.N_NAME = 'PERU') \
                    OR (N1.N_NAME = 'PERU' AND N2.N_NAME = 'KENYA')) \
                AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' \
        ) AS SHIPPING \
    group by SUPP_NATION, CUST_NATION, L_YEAR \
    order by SUPP_NATION, CUST_NATION, L_YEAR"

q16 = "select P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT \
    from PARTSUPP, PART \
    where P_PARTKEY = PS_PARTKEY \
        AND P_BRAND <> 'BRAND#34' \
        AND P_TYPE NOT LIKE 'ECONOMY BRUSHED%' \
        AND P_SIZE IN (22, 14, 27, 49, 21, 33, 35, 28) \
        AND PARTSUPP.PS_SUPPKEY NOT IN ( \
            SELECT \
                S_SUPPKEY \
            FROM \
                SUPPLIER \
            WHERE \
                S_COMMENT LIKE '%CUSTOMER%COMPLAINTS%') \
    group by P_BRAND, P_TYPE, P_SIZE \
    order by SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE"

q18 = """
    select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
    from customer, orders, lineitem
    where o_orderkey in ( select l_orderkey
                        from lineitem 
                        group by l_orderkey 
                        having sum(l_quantity) > 100 )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
    group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
    order by o_totalprice desc, o_orderdate
"""

q20 = """
    select s_name, s_address
    from supplier, nation 
    where s_suppkey in (
        select ps_suppkey
        from partsupp where ps_partkey in ( 
            select p_partkey
            from part
            where p_name like 'forest%'
        ) and ps_availqty > (
            select 0.5 * sum(l_quantity)
            from lineitem
            where l_partkey = ps_partkey
                and l_suppkey = ps_suppkey
                and l_shipdate >= date('1994-01-20')
                and l_shipdate < date('1995-01-20')
        )
    ) and s_nationkey = n_nationkey and n_name = 'CANADA'
    order by s_name
"""

q22 = """
    select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
    from ( select substr(c_phone, 1, 2) as cntrycode, c_acctbal
            from customer
            where substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
                and c_acctbal > ( select avg(c_acctbal)
                                from customer
                                where c_acctbal > 0.00 and substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
                                ) 
                and not exists ( select *
                                from orders
                                where o_custkey = c_custkey
                                )
            ) as custsale
    group by cntrycode
    order by cntrycode
"""
