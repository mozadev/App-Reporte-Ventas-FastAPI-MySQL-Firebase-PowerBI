from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from Data.data import *

# Libreria de procesamiento
import pandas as pd

import datetime
from dateutil.relativedelta import relativedelta

from pydantic import BaseModel
from typing import Optional
import json

# FIREBASE
import firebase_admin
from firebase_admin import credentials, db

firebase_sdk = credentials.Certificate(
    "Data/base-de-datos-ba-firebase-adminsdk-gjzgi-2ad58f7287.json"
)
firebase_admin.initialize_app(
    firebase_sdk,
    {"databaseURL": "https://base-de-datos-ba-default-rtdb.firebaseio.com/"},
)
# COLECCIONES
db_ventas = db.reference("/DataVentas")
db_clientes = db.reference("/DataClientes")
db_productos = db.reference("/DataProductos")

app = FastAPI()


@app.get("/")
def index():
    print("Inicio")
    return {"msg": "Pagina Principal"}


# ============================= INSERT =============================
# INSERT - VENTAS


@app.get("/Data/VentasInsert")
def VentasInsert():
    df = pd.read_excel("DatosXLS/H&M-Ventas.xlsx")
    df = df.rename(
        columns={
            "Order ID": "order_id",
            "Order Date": "order_date",
            "Ship Mode": "ship_mode",
            "Customer ID": "customer_id",
            "Country": "country",
            "City": "city",
            "State": "state",
            "Region": "region",
            "Product ID": "product_id",
            "Sales": "sales",
            "Quantity": "quantity",
            "Discount": "discount",
            "Profit": "profit",
        }
    )
    DataVentasInsert(df)
    data_json = df.to_json(orient="records")
    return Response(content=data_json, media_type="application/json")


# INSERT - CLIENTES
@app.get("/Data/ClientesInsert")
def ClientesInsert():
    df = pd.read_excel("DatosXLS/H&M-Clientes.xlsx")
    df = df.rename(
        columns={
            "Customer ID": "customer_id",
            "Nombre Cliente": "nombre_cliente",
            "Fecha Nacimiento": "fecha_nacimiento",
            "Direccion": "direccion",
            "Localidad": "localidad",
            "Telefono": "telefono",
            "Correo Electronico": "correo_electronico",
            "Fecha Alta": "fecha_alta",
            "Grupo Clientes": "grupo_cliente",
            "Genero Clientes": "genero_cliente",
        }
    )
    DataClientesInsert(df)
    data_json = df.to_json(orient="records")
    return Response(content=data_json, media_type="application/json")


# INSERT - PRODUCTOS
@app.get("/Data/ProductosInsert")
def ProductosInsert():
    df = pd.read_excel("DatosXLS/H&M-Productos.xlsx")
    df = df.rename(
        columns={
            "Product ID": "product_id",
            "Categoria": "categoria",
            "Sub Categoria": "sub_categoria",
            "Cantidad": "cantidad",
            "Costo": "costo",
            "Color": "color",
        }
    )
    DataProductosInsert(df)
    data_json = df.to_json(orient="records")
    return Response(content=data_json, media_type="application/json")


# ============================= PROCESAMIENTO PANDAS para mandarlo a  FIREBASE=============================
# VENTAS X MES
@app.get("/Data/VentasMes")
def VentasMes():
    lst = DataVentasMes()  # Traer datos de mysql
    print(lst)
    data = pd.DataFrame(lst)  # Leer los datos con pandas
    data["order_date"] = pd.to_datetime(
        data["order_date"]
    )  # Modelar la data (Dando formato a la fecha)
    data["año"] = data[
        "order_date"
    ].dt.year  # Creando columna año a partir del campo fecha
    data["mes"] = data["order_date"].dt.strftime(
        "%B"
    )  # Creando columna mes a partir del campo fecha y con formato de texto
    data["order_date"] = data["order_date"].dt.strftime(
        "%Y-%m-%d"
    )  # Dando formato al año con AÑO-MES-DIA
    print(data)
    data_json = data.to_json(orient="records", date_format="iso")  # Pasando data a JSON
    db_data = db.reference(
        "/VentasMes"
    )  # Conectando con Firebase y creando Coleccion nueva
    if db_data.get():  # Verificando si existe la coleccion
        db_data.delete()  # Borrando la coleccion en caso exista
    db_data.set(
        json.loads(data_json)
    )  # Insertando los datos a la colección de firebase
    return Response(
        content=data_json, media_type="application/json"
    )  # Retornando la respuesta con los datos json


# VENTAS X PRODUCTO
@app.get("/Data/ProductosVentas")
def ProductosVentas():
    lst = DataProductosVentas()
    data = pd.DataFrame(lst)
    data["order_date"] = pd.to_datetime(data["order_date"])
    data["order_date"] = data["order_date"].dt.strftime("%Y-%m-%d")
    print(data)
    data_json = data.to_json(orient="records")
    db_data = db.reference("/ProductosVentas")
    if db_data.get():
        db_data.delete()
    db_data.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# VENTAS CORTO PLAZO
@app.get("/Data/VentasPlazoCorto")
def VentasPlazoCorto():
    # Trayendo la data de mysql
    lst = DataVentasPlazoCorto()
    # usando pandas
    data = pd.DataFrame(lst)
    data["order_date"] = pd.to_datetime(data["order_date"])
    data["order_date"] = data["order_date"].dt.strftime("%Y-%m-%d")
    data_json = data.to_json(orient="records", date_format="iso")
    # Firebase
    db_data = db.reference("/VentasPlazoCorto")
    if db_data.get():
        db_data.delete()
    db_data.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# CATEGORIAS X VENTAS FUTURAS
@app.get("/Data/CategoriasVentas")
def CategoriasVentas():
    # Trayendo la data de mysql
    lst = DataCategoriasVentas()
    # usando pandas
    data = pd.DataFrame(lst)
    data["order_date"] = pd.to_datetime(data["order_date"])
    data["order_date"] = data["order_date"].dt.strftime("%Y-%m-%d")
    data_json = data.to_json(orient="records", date_format="iso")
    # Firebase
    db_data = db.reference("/CategoriasVentas")
    if db_data.get():
        db_data.delete()
    db_data.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# TENDENCIA DE VENTAS
@app.get("/Data/TendenciaVentas")
def TendenciaVentas():
    # Trayendo la data de mysql
    lst = DataTendenciaVentas()
    # usando pandas
    data = pd.DataFrame(lst)
    data["order_date"] = pd.to_datetime(data["order_date"])
    data["order_date"] = data["order_date"].dt.strftime("%Y-%m-%d")
    data_json = data.to_json(orient="records", date_format="iso")
    # Firebase
    db_data = db.reference("/TendenciaVentas")
    if db_data.get():
        db_data.delete()
    db_data.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# RENTABILIDAD DE ESTADOS
@app.get("/Data/RentabilidadEstado")
def RentabilidadEstado():
    # Trayendo la data de mysql
    lst = DataRentabilidadEstado()
    # usando pandas
    data = pd.DataFrame(lst)
    data["order_date"] = pd.to_datetime(data["order_date"])
    data["order_date"] = data["order_date"].dt.strftime("%Y-%m-%d")
    data_json = data.to_json(orient="records", date_format="iso")
    # Firebase
    db_data = db.reference("/RentabilidadEstado")
    if db_data.get():
        db_data.delete()
    db_data.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# ============================= EXPORTACIÓN FIREBASE =============================
# EXPORTACIÓN VENTAS
@app.get("/Data/VentasExport")
def VentasExport():
    lst = DataVentasList()
    data = pd.DataFrame(lst)
    data_json = data.to_json(orient="records")

    # Subida de data a firebase
    db_ventas.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# EXPORTACIÓN CLIENTES
@app.get("/Data/ClientesExport")
def ClientesExport():
    lst = DataClientesList()
    data = pd.DataFrame(lst)
    data_json = data.to_json(orient="records")
    # Subida de data a firebase
    db_clientes.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")


# EXPORTACIÓN PRODUCTOS
@app.get("/Data/ProductosExport")
def ProductosExport():
    lst = DataProductosList()
    data = pd.DataFrame(lst)
    data_json = data.to_json(orient="records")
    # Subida de data a firebase
    db_productos.set(json.loads(data_json))
    return Response(content=data_json, media_type="application/json")
