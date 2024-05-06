import os
import socketio
import asyncio
import pathlib
import base64
import uuid
import socketio.exceptions
from zip_file import open_zip_file
from read_ini import getConfigClient
import time

branc  = socketio.AsyncClient(logger=True, reconnection=True)
config =  getConfigClient()

async def reconnect():
    print('Attempting to reconnect...')
    try:
        await branc.connect(f'http://{config[0]}:{config[1]}', namespaces=f'/{config[2].upper()}', headers={'name': f'/{config[2].upper()}', 'serie': f'{config[3].upper()}'}, transports=['polling', 'websocket'])
        await branc.wait()
    except socketio.exceptions.ConnectionError as e:
        print(f'Error reconnecting: {e}')
        await asyncio.sleep(5)
        await reconnect()

@branc.on('connect', namespace=f'/{config[2].upper()}')
async def connect():
    try:
        print('Estoy conectado')
        await asyncio.sleep(5)
        await branc.emit('start_task', data={'hola':1},namespace=f"/{config[2].upper()}")
    except socketio.exceptions.ConnectionError:
        print('reconectando')
        await asyncio.sleep(5)
        await reconnect()
    
@branc.on('welcome', namespace=f'/{config[2].upper()}')
async def recv(data):
    print(data)

@branc.on('disconnect', namespace=f'/{config[2].upper()}')
async def on_disconnect():
    #await branc.disconnect()
    print('Desconectado del servidor')
    #await reconnect()

@branc.on('sync', namespace=f'/{config[2].upper()}')
async def my_event(data):
    #print('escuchando evento', data)
    decoded = data['message']
    print(decoded)
    random_file_name = uuid.uuid4()
    with open(f"{pathlib.Path().absolute()}\\zip\\{random_file_name}.zip", 'wb') as zip:
        zip.write(base64.b64decode(decoded))
    zip_decompress = open_zip_file(f"{pathlib.Path().absolute()}\\zip\\{random_file_name}.zip", pathDecompress=f"{pathlib.Path().absolute()}\\tmp")
    if zip_decompress:
        os.remove(f"{pathlib.Path().absolute()}\\zip\\{random_file_name}.zip")
        print('archivo eliminado' )



async def init_client():
    try:
        await branc.connect(f'http://{config[0]}:{config[1]}', namespaces=f'/{config[2].upper()}', headers={'name': f'/{config[2].upper()}', 'serie': f'{config[3].upper()}'}, transports=['polling', 'websocket'])
        await branc.wait()
    except Exception as e:
        print("por aqui el primer error: {}".format(e))    
        while not branc.connected:
            try:
                await branc.connect(f'http://{config[0]}:{config[1]}', namespaces=f'/{config[2].upper()}', headers={'name': f'/{config[2].upper()}', 'serie': f'{config[3].upper()}'}, transports=['polling', 'websocket'])
                await branc.wait()
            except Exception as e:
                print(e)
                time.sleep(3)    

if __name__ == '__main__':
    if not os.path.exists(f"{pathlib.Path().absolute()}\\tmp"):
        os.mkdir(f"{pathlib.Path().absolute()}\\tmp")
    if not os.path.exists(f"{pathlib.Path().absolute()}\\zip"):
        os.mkdir(f"{pathlib.Path().absolute()}\\zip") 
    asyncio.get_event_loop().run_until_complete(init_client())
    #asyncio.get_event_loop().run_until_complete(init_client())

