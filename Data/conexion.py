import mysql.connector

def mysql_connection():
    conn = mysql.connector.connect(
        host='localhost',
        user='admin',
        password='admin',
        database='data_sales_hm'
    )
    return conn

