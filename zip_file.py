import zipfile
import os

def compress_file(name_file:str, path:str):

    dirPathPattern = r'{}'.format(path)
    result = next(os.walk(dirPathPattern))[2]
    if len(result) > 0:
        file_compress = zipfile.ZipFile(name_file, 'w')
        print(result)
        for file in result:
            if file.endswith(('.idx', '.dat', '.blb')):
                print(file)
                file_compress.write(filename=f"{ file}",  arcname=file, compress_type=zipfile.ZIP_DEFLATED)
                print(dirPathPattern+ file)
        file_compress.close()
        return True
    else:
        return None    
    
def open_zip_file(name_file:str, pathDecompress):
    """"""
    name_file = name_file
    zip_ = zipfile.ZipFile(name_file, 'r')
    try:
        zip_.extractall(path=pathDecompress)
        return True
    except  Exception as e:
        print(e)  
        return False  

