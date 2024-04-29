import configparser
import asyncio
#import os

async def lastAuto():
    global_var = configparser.ConfigParser()
    global_var.read('server.ini')
    auto =  global_var.items('LASTAUTO')
    return auto

def getKeys():
    get_keys = configparser.ConfigParser()
    get_keys.read('server.ini')
    
    return get_keys.options('LASTAUTO')

async def updateAuto(data, id_caja, section):
    try:
        var = configparser.ConfigParser()
        var.read('server.ini')
        var.set(section, id_caja, data)

        with open('server.ini', 'w') as configfile:
            var.write(configfile)
    except Exception as e:
        print(e)        

def getServerConfig():
    server = configparser.ConfigParser()
    server.read('server.ini')
    ip = server.get('CONFIG', 'IP')
    port = server.get('CONFIG', 'PORT')

    return ip, port

def getConfigClient():
    client = configparser.ConfigParser()
    client.read('client.ini')
    ip = client.get('CONFIG', 'SERVERIP')
    port = client.get('CONFIG', 'PORT')
    client_name = client.get('CONFIG', 'CLIENTNAME')
    serie = client.get('CONFIG', 'SERIE')
    return ip, port, client_name.lower(), serie
    

#updateAuto('25', 'CAJA01', 'LASTAUTO')    