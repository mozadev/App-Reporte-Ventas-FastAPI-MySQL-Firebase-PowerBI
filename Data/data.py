from Data.conexion import mysql_connection
import pandas as pd
from datetime import date
from Data.data import *
import datetime
from dateutil.relativedelta import relativedelta
# ============================= INSERT =============================
# INSERT - VENTAS
def DataVentasInsert(df):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        # Verifica si la tabla "ventas" existe
        cursor.execute("SHOW TABLES LIKE 'ventas'")
        table_exists = cursor.fetchone()

        if table_exists:
            # Si la tabla existe, borra todos los datos
            cursor.execute("DELETE FROM ventas")
            print("Datos existentes en la tabla 'ventas' eliminados.")

        # Itera a través de tus datos y ejecuta consultas de inserción
        for index, row in df.iterrows():
            query = f"INSERT INTO ventas (order_id, order_date, ship_mode, customer_id, country, city, state, region, product_id, sales, quantity, discount, profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['order_id'], row['order_date'], row['ship_mode'], row['customer_id'], row['country'], row['city'], row['state'], row['region'], row['product_id'], row['sales'], row['quantity'], row['discount'], row['profit'])
            cursor.execute(query, values)
        # Confirma los cambios en la base de datos
        conn.commit()
    except Exception as e:
        print(f"Error al insertar datos en la base de datos: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# INSERT - CLIENTES 
def DataClientesInsert(df):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        # Verifica si la tabla "ventas" existe
        cursor.execute("SHOW TABLES LIKE 'clientes'")
        table_exists = cursor.fetchone()
        if table_exists:
            # Si la tabla existe, borra todos los datos
            cursor.execute("DELETE FROM clientes")
            print("Datos existentes en la tabla 'clientes' eliminados.")
        # Itera a través de tus datos y ejecuta consultas de inserción
       
       
        for index, row in df.iterrows():
            #df.fecha_nacimiento = df.fecha_nacimiento.astype(str)

            edad = str(2023- row['fecha_nacimiento'].year)
            print(edad)
            query = f"INSERT INTO clientes (customer_id, nombre_cliente, fecha_nacimiento, direccion, localidad, telefono, correo_electronico, fecha_alta, grupo_cliente, genero_cliente, edad_cliente) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            values = (row['customer_id'], row['nombre_cliente'], row['fecha_nacimiento'], row['direccion'], row['localidad'], row['telefono'], row['correo_electronico'], row['fecha_alta'], row['grupo_cliente'], row['genero_cliente'], edad)
            cursor.execute(query, values)
        # Confirma los cambios en la base de datos
        conn.commit()
    except Exception as e:
        print(f"Error al insertar datos en la base de datos: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# INSERT - PRODUCTOS 
def DataProductosInsert(df):
    try:
        conn = mysql_connection()
        cursor = conn.cursor()
        # Verifica si la tabla "ventas" existe
        cursor.execute("SHOW TABLES LIKE 'productos'")
        table_exists = cursor.fetchone()
        if table_exists:
            # Si la tabla existe, borra todos los datos
            cursor.execute("DELETE FROM productos")
            print("Datos existentes en la tabla 'productos' eliminados.")
        # Itera a través de tus datos y ejecuta consultas de inserción
        for index, row in df.iterrows():
            query = f"INSERT INTO productos (product_id, categoria, sub_categoria, cantidad, costo, color) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (row['product_id'], row['categoria'], row['sub_categoria'], row['cantidad'], row['costo'], row['color'])
            cursor.execute(query, values)
        # Confirma los cambios en la base de datos
        conn.commit()
    except Exception as e:
        print(f"Error al insertar datos en la base de datos: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================= LISTADO A EXPORTAR =============================
# LISTADO - VENTAS 
def DataVentasList():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"SELECT * FROM ventas;"
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "idventa": row[0],
            "order_id": row[1],
            "order_date": row[2],
            "ship_mode": row[3],
            "customer_id": row[4],
            "country": row[5],
            "city": row[6],
            "state": row[7],
            "region": row[8], 
            "product_id": row[9],
            "sales": row[10], 
            "quantity": row[11],
            "discount": row[12], 
            "profit": row[13]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# LISTADO - CLIENTES 
def DataClientesList():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"SELECT * FROM clientes;"
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "customer_id": row[0],
            "nombre_cliente": row[1],
            "fecha_nacimiento": row[2],
            "direccion": row[3],
            "localidad": row[4],
            "telefono": row[5],
            "correo_electronico": row[6],
            "fecha_alta": row[7],
            "grupo_cliente": row[8]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# LISTADO - PRODUCTOS 
def DataProductosList():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"SELECT * FROM productos;"
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "product_id": row[0],
            "categoria": row[1],
            "sub_categoria": row[2],
            "cantidad": row[3],
            "costo": row[4],
            "color": row[5]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# ============================= CONSULTA PROCESADA EN MYSQL =============================
# VENTAS X MES
def DataVentasMes():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"select v.order_date, v.product_id, v.sales, v.quantity, p.sub_categoria, p.costo from ventas as v inner join productos as p on v.product_id=p.product_id;"
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "order_date": row[0],
            "product_id": row[1],
            "sales": row[2], 
            "quantity": row[3],
            "sub_categoria": row[4], 
            "costo": row[5]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# VENTAS X PRODUCTO
def DataProductosVentas():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"select v.order_date, v.product_id, v.sales, v.quantity, p.sub_categoria, p.costo from ventas as v inner join productos as p on v.product_id=p.product_id;"
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "order_date": row[0],
            "product_id": row[1],
            "sales": row[2], 
            "quantity": row[3],
            "sub_categoria": row[4], 
            "costo": row[5]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# VENTAS A CORTO PLAZO
def DataVentasPlazoCorto():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = f"""SELECT v.idventa, v.order_id, v.order_date, v.ship_mode, v.customer_id, v.country, v.city, v.state, v.region, v.product_id,  v.sales, v.quantity, v.discount, v.profit, p.categoria, p.sub_categoria, c.genero_cliente, c.edad_cliente from ventas as v
        inner join productos as p on v.product_id=p.product_id 
        inner join clientes as c on c.customer_id=v.customer_id;"""
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "idventa": row[0],
            "order_id": row[1],
            "order_date": row[2],
            "ship_mode": row[3],
            "customer_id": row[4],
            "country": row[5],
            "city": row[6],
            "state": row[7],
            "region": row[8], 
            "product_id": row[9],
            "sales": row[10], 
            "quantity": row[11],
            "discount": row[12], 
            "profit": row[13],
            "categoria": row[14],
            "sub_categoria": row[15],
            "genero_cliente": row[16],
            "edad_cliente": row[17]

        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# CLIENTES X VENTAS FUTURAS
def DataCategoriasVentas():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = """select v.order_date,c.customer_id,c.nombre_cliente,p.categoria,p.sub_categoria, v.sales, c.genero_cliente, c.edad_cliente from ventas as v
                inner join clientes as c on v.customer_id=c.customer_id
                inner join productos as p on v.product_id=p.product_id;"""
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "order_date": row[0],
            "customer_id": row[1],
            "nombre_cliente": row[2],
            "categoria": row[3],
            "sub_categoria": row[4],
            "sales": row[5],
            "genero_cliente": row[6],
            "edad_cliente": row[7]

        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# CLIENTES X VENTAS FUTURAS
def DataTendenciaVentas():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = """select v.order_date,v.region,v.state,v.city,p.product_id,p.sub_categoria,v.sales from ventas as v
                inner join productos as p on v.product_id=p.product_id;"""
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "order_date": row[0],
            "region": row[1],
            "state": row[2],
            "city": row[3],
            "product_id": row[4],
            "sub_categoria": row[5],
            "sales": row[6]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data

# RENTABILIDAD DE ESTADOS
def DataRentabilidadEstado():
    conn = mysql_connection()
    cursor = conn.cursor()
    query = """select order_date,p.product_id,v.state,v.city,p.sub_categoria,p.costo,v.quantity,v.sales,v.profit from ventas as v
                inner join productos as p on v.product_id=p.product_id;"""
    cursor.execute(query)
    resultado = cursor.fetchall()
    data = []
    for row in resultado:
        lst = {
            "order_date": row[0],
            "product_id": row[1],
            "state": row[2],
            "city": row[3],
            "sub_categoria": row[4],
            "costo": row[5],
            "quantity": row[6],
            "sales": row[7],
            "profit": row[8]
        }
        data.append(lst)
    cursor.close()
    conn.close()
    return data
