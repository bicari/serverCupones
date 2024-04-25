import asyncio
import pyodbc
import pathlib
from datetime import datetime
from read_ini import lastAuto, updateAuto

async def query_operacion_detalle(serie, name: str):
    with pyodbc.connect("DSN=A2GKC") as connection:
        cursor = connection.cursor()
        row=cursor.execute(f"""SELECT MAX(FTI_AUTOINCREMENT), FTI_SERIE
                                FROM SOPERACIONINV 
                                WHERE FTI_STATUS = 1  AND FTI_TIPO = 11 AND FTI_SERIE = '{serie}' 
                                AND FTI_FECHAEMISION = '{str(datetime.date(datetime.now()))}' 
                                GROUP BY FTI_SERIE""").fetchone()
        auto = await lastAuto(name)  
        #print(auto, row)                               
        if row != None and row[0] > int(auto):
                #print(row)
                cursor.execute(f""" SELECT 
                                            FTI_AUTOINCREMENT              AS AUTO, 
                                            FTI_SERIE                      AS SERIE, 
                                            FTI_DOCUMENTO                  AS DOCUMENTO,
                                            CAST(FTI_FECHAEMISION AS DATE) AS FECHA,
                                            FTI_RESPONSABLE                AS CLIENTECOD,
                                            FTI_PERSONACONTACTO            AS CLIENTENOM,
                                            FTI_TELEFONOCONTACTO           AS CLIENTETLF,
                                            FTI_FACTORREFERENCIA           AS TASA,
                                            FTI_TOTALNETO                  AS TOTALBS,
                                            CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) AS MONEY)        AS TOTALUSD0, 
   ROUND(CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) +  ((FTI_BASEIGTF * 0.03) / FTI_FACTORREFERENCIA) AS MONEY)) AS TOTALUSD1, 
   CAST((FTI_TOTALNETO / FTI_FACTORREFERENCIA) + ((FTI_BASEIGTF * 0.03) / FTI_FACTORREFERENCIA) AS MONEY)         AS TOTALUSD2,
   ROUND(CAST((FTI_TOTALBRUTO / FTI_FACTORREFERENCIA) AS MONEY))                                                  AS TOTALUSD3, 
   CAST((FTI_TOTALBRUTO / FTI_FACTORREFERENCIA) AS MONEY)                                                         AS TOTALUSD4,
    
                                            FTI_SALDOOPERACION AS SALDO,
                                            FTI_TOTALITEMS AS ITEMS, FTI_MACHINENAME AS MAQUINA,
                                             CAST(FTI_HORA AS VARCHAR(8)) AS HORA
                                    INTO "{pathlib.Path().absolute()}\\tmp\\SoperacionINV{name}"
                                    FROM SOPERACIONINV
                                    WHERE FTI_AUTOINCREMENT = {row[0]}""")
                cursor.execute(f"""SELECT 
                                        FDI_DOCUMENTO,
                                        FDI_CODIGO,
                                        FDI_CLIENTEPROVEEDOR,
                                        FDI_CANTIDAD,
                                        FDI_FECHAOPERACION
                                    INTO "{pathlib.Path().absolute()}\\tmp\\SDetalleventa{name}"     
                                    FROM SDETALLEVENTA
                                    WHERE FDI_OPERACION_AUTOINCREMENT = {row[0]} """)
                await updateAuto(str(row[0]), name.upper(), 'LASTAUTO')
                return True
        else:
             return False