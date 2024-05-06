import asyncio
import pyodbc
import pathlib
from datetime import datetime
from read_ini import lastAuto, updateAuto
from send_data_client import send_Data

async def query_operacion_detalle(serie, name:str):
    with pyodbc.connect("DSN=A2GKC") as connection:
        try:
            cursor = connection.cursor()
            row=cursor.execute(f"""SELECT MAX(FTI_AUTOINCREMENT), FTI_SERIE
                                FROM SOPERACIONINV 
                                WHERE FTI_STATUS = 1  AND FTI_TIPO = 11 AND FTI_SERIE IN {serie} 
                                AND FTI_FECHAEMISION = '{str(datetime.date(datetime.now()))}' 
                                GROUP BY FTI_SERIE""").fetchall()
            #auto = await lastAuto(serie)
        except Exception as e:
             print(e)   
             return []
        return row
async def test():
     print('Simulando consulta')

async def search_soperacion_sedetalle(autoincrement, name, serie):
        
        print('funcit')                   
        with pyodbc.connect("DSN=A2GKC") as connection:#print(auto, row) 
            try:
                cursor = connection.cursor()
                cursor.execute(f""" SELECT 
                                          FTI_AUTOINCREMENT AS AUTO,
                                          CAST(FTI_SERIE AS VARCHAR(12)) AS SERIE, 
                                          CAST(FTI_DOCUMENTO AS VARCHAR(10)) AS DOCUMENTO,
                                          CAST(FTI_FECHAEMISION AS DATE) AS FECHA,
                                          CAST(FTI_RESPONSABLE AS VARCHAR(12)) AS CLIENTECOD, 
                                          CAST(FTI_PERSONACONTACTO AS VARCHAR(60)) AS CLIENTENOM,
                                          CAST(FTI_TELEFONOCONTACTO AS VARCHAR(20)) AS CLIENTETLF,
                                          FTI_FACTORREFERENCIA AS TASA, 
                                          FTI_TOTALNETO AS TOTALBS,

                                          CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) AS MONEY) AS TOTALUSD0,
  
  ROUND(CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) + ((FTI_BASEIGTF * 0.03) / FTI_FACTORREFERENCIA) AS MONEY)) AS TOTALUSD1,
        CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) + ((FTI_BASEIGTF * 0.03) / FTI_FACTORREFERENCIA) AS MONEY) AS TOTALUSD2,

  ROUND(CAST(((FTI_TOTALBRUTO - (FTI_DESCUENTO1MONTO + FTI_DESCUENTO2MONTO + FTI_DESCUENTOPARCIAL)) / FTI_FACTORREFERENCIA) AS MONEY)) AS TOTALUSD3, 
        CAST(((FTI_TOTALBRUTO - (FTI_DESCUENTO1MONTO + FTI_DESCUENTO2MONTO + FTI_DESCUENTOPARCIAL)) / FTI_FACTORREFERENCIA) AS MONEY) AS TOTALUSD4,  
  
                                          FTI_SALDOOPERACION AS SALDO,
                                          FTI_TOTALITEMS AS ITEMS,
                                          CAST(FTI_MACHINENAME AS VARCHAR(20)) AS MAQUINA,
                                          CAST(FTI_HORA AS VARCHAR(8)) AS HORA
                                    INTO "{pathlib.Path().absolute()}\\tmp\\SoperacionINV{name}"
                                    FROM SOPERACIONINV
                                    WHERE FTI_AUTOINCREMENT = {autoincrement}""")
                cursor.execute(f"""SELECT
                                    FDI_OPERACION_AUTOINCREMENT AS AUTOINC,
                                    FDI_CODIGO, 
                                    FDI_CANTIDAD,
                                    FDI_CANTIDAD * FDI_PRECIODEVENTA * (1-(FDI_PORCENTDESCUENTO1/100)) * (1-(FDI_PORCENTDESCUENTO2/100)) AS FDI_PRECIODEVENTA,
                                    FDI_CANTIDAD * FDI_PRECIOCONDESCUENTO * (1-(FDI_PORCENTDESCUENTO1/100)) * (1-(FDI_PORCENTDESCUENTO2/100)) AS FDI_PRECIOCONDESCUENTO
                                INTO "{pathlib.Path().absolute()}\\tmp\\SDetalleventa{name}"     
                                FROM SDETALLEVENTAs
                                INNER JOIN SOPERACIONINV ON FDI_OPERACION_AUTOINCREMENT = FTI_AUTOINCREMENT
                                WHERE FDI_OPERACION_AUTOINCREMENT = {autoincrement}""")
                cursor.close()
            except Exception as e:
                 print(e)
                 await updateAuto(str(autoincrement), serie.upper(), 'LASTAUTO')
                 return (False, 0)    
        encode = await send_Data(name)
        await updateAuto(str(autoincrement), serie.upper(), 'LASTAUTO')
        return True, serie, encode
        
            