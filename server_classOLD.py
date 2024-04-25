import socketio
import asyncio
import pyodbc
from datetime import datetime
import pathlib
import base64
import os
from zipfile import *
from querys_sql import query_operacion_detalle
import logging
from colorlog import ColoredFormatter
from read_ini import getServerConfig
from threading import Thread

#logging.basicConfig(format='%(levelname)s:  %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
console_handler = logging.StreamHandler()
formatter = ColoredFormatter("%(log_color)s%(levelname)s: %(message)s", log_colors={'INFO': 'green',
        'WARNING': 'yellow',})
console_handler.setFormatter(formatter)
logger=logging.getLogger('server.uvi')
logger.addHandler(console_handler)
config = getServerConfig()
sio = socketio.AsyncServer(async_mode='asgi')
app = socketio.ASGIApp(sio)



class Namespace1(socketio.AsyncNamespace):

    def __init__(self, namespace=None):
        super().__init__(namespace)
        self.name = namespace

    async def compress_file(self, name_file:str, path:str):

        dirPathPattern = r'{}'.format(path)
        print('por aqui')
        result = next(os.walk(dirPathPattern))[2]
        if len(result) > 0:
          print(name_file)
          with ZipFile(name_file, mode='w') as file_compress:
            try:
                for file in result:
                    if file.endswith(('.idx', '.dat', '.blb')):
                        os.chmod(dirPathPattern+file, 0o777)
                        print(file[:13] + file[19:23])
                        file_compress.write(f"{dirPathPattern+file}", arcname=file[:13] + file[19:23], compress_type=ZIP_DEFLATED)
                   
            except Exception as e:
                print(e)
            return True
        else:
            return None            
    
    async def on_connect(self, sid, environ):
        logger.setLevel(logging.INFO)
        headers_client_name  = environ['asgi.scope']['headers'][1][1].decode('utf-8')
        headers_client_serie = environ['asgi.scope']['headers'][2][1].decode('utf-8')
        await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace=self.name)
        logger.info(f'User:{self.name} connected')
        self.task = asyncio.create_task(self.background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente

    async def background_query_sales(self, name, serie): #Tarea en segundo plano:
        logger.setLevel(logging.INFO)
        logger.info(f'Iniciando tarea en segundo plano usuario: {self.name}')
        
        while True:
            row = await query_operacion_detalle(serie, name= self.name[1:])
            #print(row)
            if row == True:
                path_script = f"{os.path.dirname(__file__)}"
                new_path = pathlib.PureWindowsPath(path_script)
                compress = await self.compress_file(f'{pathlib.Path().absolute()}\\zip\\{self.name[1:]}.zip', f"{str(new_path)}\\tmp\\")
                if compress:
                    try:
                        with open(f"{pathlib.Path().absolute()}\\zip\\{self.name[1:]}.zip", 'rb') as file:
                            bytes = file.read()
                            encoded = base64.b64encode(bytes)
                        await sio.emit('sync', {'message': encoded}, namespace=name)
                    except Exception as e:
                        print(e)    
            
            logger.info(f'Usuario:{self.name} Sin cambios en base de datos')    
            #compress =  compress_file(f'{env_var[5]}.zip', f"{pathlib.Path().absolute()}\\"
            
            await asyncio.sleep(5)

    async def on_disconnect(self, sid):
        logger.setLevel(logging.INFO)
        logger.info(f'Usuario:{self.name} se ha desconectado')
        var=self.task.cancel()
        


    async def on_message(self, sid, data):
        print('Namespace1: Message received:', data)


sio.register_namespace(Namespace1('/caja01'))
sio.register_namespace(Namespace1('/caja02'))
sio.register_namespace(Namespace1('/caja03'))
sio.register_namespace(Namespace1('/caja04'))
sio.register_namespace(Namespace1('/caja05'))
sio.register_namespace(Namespace1('/caja06'))
sio.register_namespace(Namespace1('/caja07'))
sio.register_namespace(Namespace1('/caja08'))


if __name__ == '__main__':
    try:
        if not os.path.exists(f"{pathlib.Path().absolute()}\\tmp"):
            os.mkdir(f"{pathlib.Path().absolute()}\\tmp")
        if not os.path.exists(f"{pathlib.Path().absolute()}\\zip"):
            os.mkdir(f"{pathlib.Path().absolute()}\\zip")        
        import uvicorn
        config = uvicorn.Config(app, host=config[0] ,port=config[1], workers=8)
        server = uvicorn.Server(config)
        server.run()
        #uvicorn.run(app, host=config[0], port=int(config[1]), workers=8)
    except KeyboardInterrupt as e:
        logger.warning("Cerrando Servidor http, se desconectaran todas las sesiones establecidas")
        