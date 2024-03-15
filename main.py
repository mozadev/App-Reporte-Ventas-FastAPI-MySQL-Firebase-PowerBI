import schedule
import time
import threading
import requests
from fastapi import FastAPI
from Controllers.controllers import app as controllers_app
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.include_router(controllers_app.router, prefix="")


# Función para programar la tarea de exportación
def schedule_export_data():
    schedule.every().day.at("00:25").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/VentasInsert")
    )
    schedule.every().day.at("00:25").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/ClientesInsert")
    )
    schedule.every().day.at("00:25").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/ProductosInsert")
    )
    schedule.every().day.at("00:30").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/VentasMes")
    )
    schedule.every().day.at("00:32").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/ProductosVentas")
    )
    schedule.every().day.at("00:34").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/VentasPlazoCorto")
    )
    schedule.every().day.at("00:36").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/CategoriasVentas")
    )
    schedule.every().day.at("00:38").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/TendenciaVentas")
    )
    schedule.every().day.at("00:40").do(
        lambda: requests.get("http://192.168.0.102:8000/Data/RentabilidadEstado")
    )


# Función que ejecuta la tarea de exportación cada hora
def run_export_data():
    while True:
        schedule.run_pending()
        time.sleep(1)


# Programar la tarea de exportación
schedule_export_data()


# Iniciar el servidor FastAPI en un hilo separado
def start_server():
    uvicorn.run(app, host="192.168.0.3", port=8000)


# Ejecutar el servidor FastAPI y la tarea de exportación en hilos separados
if __name__ == "__main__":
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    export_thread = threading.Thread(target=run_export_data, daemon=True)
    export_thread.start()
    # Mantén el programa en ejecución hasta que se presione Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
