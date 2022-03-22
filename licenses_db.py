import sqlite3
from sqlite3 import Error
import pandas as pd
import mysql.connector


def create_table(table,col1,col2,records):

    try:
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Chimica90$",
        database="testdatabase")

        c=mydb.cursor()

        sql_create=""" CREATE TABLE {0} ({1} text, {2} text) """.format(table,col1,col2)

        c.execute(sql_create)

        sql_insert = """ INSERT INTO {0} ({1}, {2}) VALUES (%s, %s) """.format(table, col1, col2)

        c.executemany(sql_insert, records)

        mydb.commit()
        print("Date Record inserted successfully")

        splitted=f"splitted_{table[0:2]}"

        query_union1 = """ CREATE TABLE temp_union1 AS SELECT {1}, SUBSTRING_INDEX({2},',',1) as {2} FROM {0} \
        UNION SELECT {1}, SUBSTRING_INDEX(SUBSTRING_INDEX({2},',',2),',',-1) FROM {0} """.format(table,col1,col2)

  #      print(query_union1)
        c.execute(query_union1)
        query_union2 = """ CREATE TABLE temp_union2 AS SELECT {1}, SUBSTRING_INDEX(SUBSTRING_INDEX({2},',',3),',',-1) as {2} FROM {0} \
        UNION SELECT {1}, SUBSTRING_INDEX(SUBSTRING_INDEX({2},',',4),',',-1) FROM {0} """.format(table,col1,col2)

  #      print(query_union2)
        c.execute(query_union2)
        query_union3=""" CREATE TABLE {2} AS SELECT {0}, {1} FROM temp_union1 \
        UNION SELECT {0}, {1} FROM temp_union2 """.format(col1,col2,splitted)

        c.execute(query_union3)
        c.execute("""drop table temp_union1""")
        c.execute("""drop table temp_union2""")
        print("Splitting executed ...")

        mydb.commit()

    except mysql.connector.Error as error:
      mydb.rollback()
      print("Failed to insert into MySQL table {}".format(error))
    return mydb,splitted

def check_licenses(table1, table2):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Chimica90$",
        database="testdatabase")

    c = mydb.cursor()
    query_join=""" CREATE TABLE new_table as SELECT {0}.user_name, {1}.product_name FROM {0} INNER \
 JOIN {1} ON {0}.product_licenses={1}.product_licenses """.format(table1,table2)
    c.execute(query_join)

    query_user= """ SELECT DISTINCT user_name FROM new_table"""


    c.execute(query_user)

    query_user = c.fetchall()

    users = []
    for item in query_user:
        users.append(item[0])


    query_product_lic = """ SELECT DISTINCT product_name FROM new_table"""

    c.execute(query_product_lic)

    query_product_lic=c.fetchall()

    products=[]

    for item in query_product_lic:
        products.append(item[0])

    stats=['True']*len(products)

    outcome=list(zip(users,products,stats))

    for i in outcome:
        print(i)

if __name__=='__main__':
    users = [('Patrick Miller','0pd3z81168,0h72PQzGzq,12dBuZZB4N'),\
             ('Zac Patel','b9jsp75JFj,IbHKpn7732,pU85XxhyH5')]
    products = [('Benifema', '0h72PQzGzq'), ('Jutafenac','12dBuZZB4N,NHe2NB5ugq,0pd3z81168,pU85XxhyH5'),\
                ('Lipsin','z3orr4T82y,AK1PQYA1Vp,11N9mnOaxT'),('Raxone','Oi48ZGe5Q7')]

    (db, splitted_pr) = create_table('products', 'product_name',
                 'product_licenses',
                 products)

    (db, splitted_us) = create_table('users','user_name','product_licenses',users)
    check_licenses(splitted_us, splitted_pr)




#I want to seperate delimited string in a column into rows

# ID Value           ID Value
# ---------          ----------
# 1  a,b     into    1  a
# 2  c,d             1  b
#                    2  c
#                    2  d

# query_union1=""" CREATE TABLE temp_union1 AS SELECT product_name, SUBSTRING_INDEX(product_licenses,',',1) FROM products\
# UNION SELECT product_name, SUBSTRING_INDEX(SUBSTRING_INDEX(product_licenses,',',2),',',-1) FROM products""";
#
# query_union2=""" CREATE TABLE temp_union2 AS SELECT product_name, SUBSTRING_INDEX(product_licenses,',',1) as ttt FROM products\
#  UNION SELECT product_name, SUBSTRING_INDEX(SUBSTRING_INDEX(product_licenses,',',2),',',-1) FROM products """;
#
#
#
# CREATE TABLE splitted AS SELECT product_name, product_licenses FROM temp_union1\
#     UNION SELECT product_name,product_licenses FROM temp_union2 ;


