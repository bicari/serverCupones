import pyodbc
import configparser
import os

####SCRIPT PARA SER EJECUTADO UNA VEZ, ANTES DE INICIAR EL SERVIDOR UVICORN
def query_max_auto():
    with pyodbc.connect("DSN=A2KSA") as connect:
        cursor=connect.cursor()
        data_max_auto=cursor.execute("""SELECT 
                            FTI_SERIE,  MAX(FTI_AUTOINCREMENT)
                        FROM SOPERACIONINV
                        WHERE FTI_TIPO = 11 AND FTI_STATUS = 1
                        GROUP BY FTI_SERIE
                       """).fetchall()
        return data_max_auto
    
def create_ini_server():
    if not os.path.exists('server.ini'):
        data = query_max_auto()
        with open("server.ini", 'w') as file:
            print('CREADO')
            server_ini = configparser.ConfigParser()
            server_ini.add_section('LASTAUTO')
            for x in data:
                server_ini.set('LASTAUTO',  str(x[0]), str(x[1]))#X[0] ES LA SERIE X[1] ES EL MAX AUTO ENCONTRADO
            server_ini.add_section('CONFIG')
            server_ini.set('CONFIG', 'IP', '0.0.0.0')
            server_ini.set('CONFIG', 'PORT', '8000')
            server_ini.write(file)
            
              
if __name__ == '__main__':    
    create_ini_server()
        
