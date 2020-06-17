from Query3_Index import *
from datetime import date
import dbm
import collections
import struct

query3_index_dbm = dbm.open('q33','r')
recordformat = '20s20s70s40s80s25s3i12s25s50s50s'
recordsize = struct.calcsize(recordformat)

def helper_sub_function(data):
    sizePerBlock = 405
    primelist=[]
    collectedResult = collections.namedtuple('KeyPersonDetails','firstName lastName job company address phone_number birth_date birth_month birth_year ssn username email url')
    while True:
        if not data[sizePerBlock:]:
            break
        subList = []
        unpacked_data = struct.unpack(recordformat, data[:sizePerBlock])
        for iteration in unpacked_data:
            if (type(iteration) != int):
                subList.append(str(iteration.decode('utf-8')).replace('\x00', ''))
            else:
                subList.append(iteration)
        primelist.append(collectedResult(*subList))
        data = data[sizePerBlock:]
    return primelist



#This Function reads the whole binary file and call the helper function for passing block to helper function
def binary_input(block ,record):
    file_open_pointer_3 = open("small.bin", 'rb')
    maxSizePerBlock = 4096
    data = file_open_pointer_3.read(maxSizePerBlock*block)
    dataoffset = data[ maxSizePerBlock * (block-1): maxSizePerBlock*block]
    preprecords = helper_sub_function(dataoffset)
    print(preprecords[record-1])


i = query3_index_dbm.firstkey()
while i != None:
    days_in_year = 365.2425
    birthDate = (date(int(i[0:4]), int(i[4:6]), int(i[6:8])))
    age = int((date.today() - birthDate).days / days_in_year)
    if (age < 21):
        b = query3_index_dbm[i].decode('utf-8')
        k = b.split(',')
        for each in k:
            block,record = each.split('-')
            binary_input(int(block) , int(record))
    i = query3_index_dbm.nextkey(i)

query3_index_dbm.close()
