import socketio
import pyodbc
import queue
import asyncio
import socketio.asgi
import socketio.async_server
import pathlib
from zip_file import compress_file
from datetime import datetime
sio = socketio.AsyncServer(async_mode='asgi', logger=True)
app = socketio.asgi.ASGIApp(sio)
global task
task = ''


@sio.event(namespace='/caja01')
async def connect(sid, enviro):
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja01')
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    global task
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja02')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja02')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja03')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja03')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja04')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja04')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente
    
@sio.event(namespace='/caja05')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja05')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja06')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja06')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja07')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja07')
    global task   
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja08')
async def connect(sid, enviro):
    headers_client_name  = enviro['asgi.scope']['headers'][1][1].decode('utf-8')
    headers_client_serie = enviro['asgi.scope']['headers'][2][1].decode('utf-8')
    await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace='/caja08')
    global task 
    task = asyncio.create_task(background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

@sio.event(namespace='/caja01')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente

@sio.event(namespace='/caja02')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente

@sio.event(namespace='/caja03')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente

@sio.event(namespace='/caja04')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente 

@sio.event(namespace='/caja05')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente 

@sio.event(namespace='/caja06')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente

@sio.event(namespace='/caja07')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente 

@sio.event(namespace='/caja08')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente 

@sio.event(namespace='/caja09')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente 

@sio.event(namespace='/caja10')
async def disconnect(sid):
    global task
    task.cancel() #Cancelando tarea en segundo plano segun la desconexion del cliente  
     
async def background_query_sales(name, serie): #Tarea en segundo plano:
    connect = pyodbc.connect("DSN=A2GKC")
    cursor  = connect.cursor()
    print('Iniciando tarea en segundo plano')

    while True:
        query=cursor.execute(f"""SELECT MAX(FTI_AUTOINCREMENT) AS AUTO 
                                FROM SOPERACIONINV 
                                WHERE FTI_STATUS = 1  AND FTI_TIPO = 11 AND FTI_SERIE = '{serie}' 
                                AND FTI_FECHAEMISION = '{str(datetime.date(datetime.now()))}' """).fetchall()
        compress_file('test.zip', f"{pathlib.Path().absolute()}")
        #print(query[0][0])
        await sio.emit('welcome', {'message': f'{query[0][0]}'}, namespace=name)
        await asyncio.sleep(5)
    
