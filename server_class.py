import socketio
import asyncio
from datetime import datetime
import pathlib
import os
from prometheus_client import start_http_server, Summary, Info, Counter
import uvicorn.config
import uvicorn.server
from querys_sql import query_operacion_detalle, search_soperacion_sedetalle
import logging
from colorlog import ColoredFormatter
from read_ini import getServerConfig, lastAuto, getKeys
import threading
import uuid, random

#logging.basicConfig(format='%(levelname)s:  %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
console_handler = logging.StreamHandler()
formatter = ColoredFormatter("%(log_color)s%(levelname)s: %(message)s", log_colors={'INFO': 'green',
        'WARNING': 'yellow',})
console_handler.setFormatter(formatter)
logger=logging.getLogger('server.uvi')
logger.addHandler(console_handler)
config = getServerConfig()
sio = socketio.AsyncServer(async_mode='asgi',  logger=True, always_connect=False, cors_allowed_origins = '*', Engineio_logger=True, ping_timeout=60, ping_interval=30)
app = socketio.ASGIApp(sio)
connected_clients = set()

global flag_task
global flag
flag = True
flag_task = False

class Namespace1(socketio.AsyncNamespace):
    
    def __init__(self, namespace=None ):
        
        super().__init__(namespace)
        self.name = namespace
        self.nameal = uuid.uuid4()
        self.i = Counter('test_2_{}'.format(str(random.randint(1,1000000))), documentation='Clientes_Conectados{}'.format(self.nameal))
        #self.flag =  Fals
         
        

    async def background_query_sales(self): #Tarea en segundo plano:
        await asyncio.sleep(1)
        logger.setLevel(logging.INFO)
        logger.info(f'Iniciando tarea en segundo plano usuario: ')
        global flag
        
        while flag and len(connected_clients) > 0:
            tasks = []
            tuple_series = tuple(connected_clients)
            if len(tuple_series) == 1:
                logger.info(f"""Series disponibles: {str(tuple_series).replace(',', '', 1)}""")
                tuple_series=str(tuple_series).replace(',', '', 1)
            else:    
                logger.info(f"""Series disponibles: {(tuple_series)}""")
            logger.info('Por aqui')
            row = await query_operacion_detalle(tuple_series, name='')
            last = await lastAuto()
            logger.info(f'{row}')
            logger.info(f'{last}')
            if len(row) > 0:
                logger.info('Dato encontrado')
                for serie in row:
                    for auto in last: 
                        if serie[1] in connected_clients and serie[1] == auto[0].upper() and serie[0] > int(auto[1]):
                            print(auto[1], serie[0], self.name)
                            operaciones = asyncio.create_task(search_soperacion_sedetalle(autoincrement=serie[0], name=serie[1], serie=serie[1]))
                            await asyncio.sleep(1)
                            tasks.append(operaciones)
                        else:
                            logger.info('Nada que mostrar')
                            continue
            if len(tasks) > 0:         
                results = await asyncio.gather(*tasks)
                for result in results:
                    print(result)
                    if result[0] == True:
                        await sio.emit('sync', {'message': result[2]}, namespace=f"/{result[1]}")
                        await asyncio.sleep(0.5)
            now = datetime.now()
            formatted_time = now.strftime("%H:%M:%S")
            logger.info(f'Usuario: Sin cambios en base de datos :{formatted_time}: {connected_clients}')    
            #compress =  compress_file(f'{env_var[5]}.zip', f"{pathlib.Path().absolute()}\\"

            await asyncio.sleep(5)
    

    async def on_start_task(self, sid, data):
        await sio.emit('welcome', {'message': 'aqui estoy cliente para ti'}, namespace=self.name)
        global flag_task, flag
        logger.setLevel(logging.INFO)
        if flag_task == False:
            task = asyncio.create_task(self.background_query_sales())
            flag_task = True
            flag = True

    async def on_connect(self, sid, environ):
        
        #self.flag = True
        logger.setLevel(logging.INFO)
        self.headers_client_name  = environ['asgi.scope']['headers'][1][1].decode('utf-8')
        self.headers_client_serie = environ['asgi.scope']['headers'][2][1].decode('utf-8')
        connected_clients.add(self.headers_client_serie.upper())
        self.i.inc()
        logger.info(f'User:{self.headers_client_serie} connected')
        print(connected_clients)
          
        
        #self.task = asyncio.create_task(self.background_query_sales(headers_client_name, headers_client_serie))#Iniciando tarea en segundo plano al conectarse un cliente
        #self.stop_event = threading.Event()
        #self.newThread = threading.Thread(target=self.run)
        #self.newThread.start()        
    async def on_reconnect(self, sid):
        print('reconnect')

    async def on_disconnect(self, sid):
        global flag_task, flag
        logger.setLevel(logging.INFO)
        logger.info(f'Usuario:{self.name} se ha desconectado')
        #var=self.task.cancel()
        connected_clients.discard(self.headers_client_serie)
        logger.info(f'Clientes: {connected_clients}')
        if len(connected_clients) == 0: 
            #await sio.shutdown()
            flag_task =  False
            flag = False
            logger.info('Tarea cancelada exitosamente')
        


for serie in getKeys():#Obteniendo series disponibles de las cajas, y creando instancias de namespaces
    if serie != None:
        sio.register_namespace(Namespace1(f"/{serie.upper()}"))


class ThreadQuery(Namespace1):
    def __init__(self, namespace=None):
        super().__init__(namespace)

    def start_thread(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()    
        

if __name__ == '__main__':
    try:
        if not os.path.exists(f"{pathlib.Path().absolute()}\\tmp"):
            os.mkdir(f"{pathlib.Path().absolute()}\\tmp")
        if not os.path.exists(f"{pathlib.Path().absolute()}\\zip"):
            os.mkdir(f"{pathlib.Path().absolute()}\\zip")        
        import uvicorn
        #config = uvicorn.Config(app, host=config[0] ,port=config[1],  workers=8)
        #server = uvicorn.Server(config)
        #server.run()
        #run = ThreadQuery()
        #run.start_thread()
        start_http_server(3500)
        uvicorn.run(app, host=config[0], port=int(config[1]))       
    except KeyboardInterrupt as e:
        logger.warning("Cerrando Servidor http, se desconectaran todas las sesiones establecidas")
        #run.stop(flag2=False)
        
