import asyncio
import logging
import os
import pathlib
import base64
from zipfile import *

async def compress_file(name_file:str, path:str, name):

        dirPathPattern = r'{}'.format(path)
        print('por aqui')
        result = next(os.walk(dirPathPattern))[2]
        if len(result) > 0:
          print(name_file)
          with ZipFile(name_file, mode='w') as file_compress:
            try:
                for file in result:
                    print(file[:13] + name+ file[23:27])
                    if file.endswith(('.idx', '.dat', '.blb')) and file == file[:13] + name + file[23:27]:
                        os.chmod(dirPathPattern+file, 0o777)
                        #print(file[:13] + file[23:27])
                        file_compress.write(f"{dirPathPattern+file}", arcname=file[:13] + file[23:27], compress_type=ZIP_DEFLATED)
                   
            except Exception as e:
                print(e)
            return True
        else:
            return None           


async def send_Data(name):
    path_script = f"{os.path.dirname(__file__)}"
    print('antes del comprimir')
    new_path = pathlib.PureWindowsPath(path_script)
    compress = await compress_file(f'{pathlib.Path().absolute()}\\zip\\{name}.zip', f"{str(new_path)}\\tmp\\", name)
    if compress:
        try:
            with open(f"{pathlib.Path().absolute()}\\zip\\{name}.zip", 'rb') as file:
                bytes = file.read()
                encoded = base64.b64encode(bytes)
            return encoded         
        except Exception as e:
            print(e)    
