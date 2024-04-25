import os
import socketio
import asyncio
import pathlib
import base64
import uuid
from zip_file import open_zip_file
from read_ini import getConfigClient

branc  = socketio.AsyncClient(logger=True)
config =  getConfigClient()


@branc.on('connect', namespace=f'/{config[2]}')
async def connect():
    print('Estoy conectado')

@branc.on('welcome', namespace=f'/{config[2]}')
async def recv(data):
    print(data)

@branc.on('sync', namespace=f'/{config[2]}')
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
        await branc.connect(f'http://{config[0]}:{config[1]}', namespaces=f'/{config[2]}', headers={'name': f'/{config[2]}', 'serie': f'{config[3]}'}, transports=['polling', 'websocket'], wait=True, wait_timeout=5)
        await branc.wait()
    except Exception as e:
        print(e)    

if __name__ == '__main__':
    if not os.path.exists(f"{pathlib.Path().absolute()}\\tmp"):
        os.mkdir(f"{pathlib.Path().absolute()}\\tmp")
    if not os.path.exists(f"{pathlib.Path().absolute()}\\zip"):
        os.mkdir(f"{pathlib.Path().absolute()}\\zip") 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_client())
    #asyncio.get_event_loop().run_until_complete(init_client())

