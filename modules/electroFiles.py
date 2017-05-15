import os
from collections import namedtuple
import base64

def getFileList(arg):
    path = arg[0]
    return os.listdir(path)

def writeTextFile(arg):
    name = arg[0]
    data = arg[1]
    writeType = "a"
    if (len(arg)>2):
        writeType = arg[2]
    try :
        f = open(name, writeType)
        f.write(data)
        f.close()
        return "OK"
    except :
        raise Exception("File can't be written")

def writeBinaryFile(arg):
    name = arg[0]
    data = arg[1]
    writeType = "ab"
    if (len(arg)>2):
        writeType = arg[2]
    try :
        f = open(name, writeType)
        f.write(base64.b64decode(data))
        f.close()
        return "OK"
    except :
        raise Exception("File can't be written")

def getTextFile(arg):
    name = arg[0]
    if (is_binary(name) is True):
        raise Exception("Oups! This is a binary file. Only text files are allowed!")
    else :
        f = open(name, "r")
        data = f.read()
        byteArray = bytes(data)
        f.close()
        return byteArray

def getFileSize(arg):
    name = arg[0]
    fileSize = os.stat(name)[6] # st_size - made compatible with micropython
    return {"size":fileSize, "unit":"bytes"}

def deleteFile(arg):
    name = arg[0]
    try :
        os.remove(name)
        return "OK"
    except :
        raise Exception("File don't exist or can't be removed")


def disk_usage(path):
    """Return disk usage statistics about the given path.

    Returned valus is a named tuple with attributes 'total', 'used' and
    'free', which are the amount of total, used and free space, in bytes.
    """
    p = path[0]
    st = os.statvfs(p)
    free = st.f_bavail * st.f_frsize
    total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    rslt = {"total" : str(intWithSpaces(total/1000000)), \
            "used"  : str(intWithSpaces(used/1000000)), \
            "free"  : str(intWithSpaces(free/1000000)), \
            "unit"  : "Mb" }

    return rslt

# needed for visualization of space sizes
def intWithSpaces(x):
    if type(x) not in type(0):
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = " %03d%s" % (r, result)
        #result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)

# reads file in chunks then encode them in base64 mode. Generator is returned here
def getFileStream(arg):
    file_object = open(arg[0], "r")
    if (is_binary(arg[0])):
        return (base64.b64encode(i) for i in read_in_chunks(file_object))
    else :
        return (i for i in read_in_chunks(file_object))

def is_binary(filename):
    """Return true if the given filename is binary.
    @raise EnvironmentError: if the file does not exist or cannot be accessed.
    @attention: found @ http://bytes.com/topic/python/answers/21222-determine-file-type-binary-text on 6/08/2010
    @author: Trent Mick <TrentM@ActiveState.com>
    @author: Jorge Orpinel <jorge@orpinel.com>"""
    fin = open(filename, 'rb')
    try:
        CHUNKSIZE = 1024
        while 1:
            chunk = fin.read(CHUNKSIZE)
            if '\0' in chunk: # found null byte
                return True
            if len(chunk) < CHUNKSIZE:
                break # done
    # A-wooo! Mira, python no necesita el "except:". Achis... Que listo es.
    finally:
        fin.close()

    return False

def read_in_chunks(file_object, chunk_size=528):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

callbacks = {
      "getFile":         {"call": getFileStream,  "parameters": "path, 'base64'", "description": "Get binary file"},
      "getFileSize":     {"call": getFileSize,     "parameters": "path",       "description": "Get file size in bytes"},
      "getFileList":     {"call": getFileList,     "parameters": "path",       "description": "Get file list in directory"},
      "writeTextFile":   {"call": writeTextFile,   "parameters": "path, data, waytowrite", "description": "Write data in file"},
      "writeBinaryFile": {"call": writeBinaryFile, "parameters": "path, data, waytowrite", "description": "Write data in file"}, 
      "getTextFile":     {"call": getTextFile,     "parameters": "path",       "description": "Get text file data"},
      "deleteFile":      {"call": deleteFile,      "parameters": "path",       "description": "Delete specified file"},
      "diskUsage":       {"call": disk_usage,      "parameters": "path",       "description": "Get disk space info"}
      }
