import struct
import collections
import dbm
from datetime import date
from Query4_Sort import *

Query4_DBM_Index = dbm.open('q44', 'c')
file_open_pointer = open("sorted.bin",'rb')


#This function reads the each block and return the combined results having 10 records
def helper_sub_function(data):
    sizePerBlock = 405
    primelist=[]
    recordoffset = 1
    collectedResult = collections.namedtuple('KeyPersonDetails', 'firstName lastName job company address phone_number birth_date birth_month birth_year ssn username email url')
    while True:
        if not data[sizePerBlock:]:
            break
        subList = []
        unpacked_data = struct.unpack('20s20s70s40s80s25s3i12s25s50s50s', data[:sizePerBlock])
        for iteration in unpacked_data:
            if (type(iteration) != int):
                subList.append(str(iteration.decode('utf-8')).replace('\x00', ''))
            else:
                subList.append(iteration)
        primelist.append((collectedResult(*subList),recordoffset))
        recordoffset += 1
        data = data[sizePerBlock:]
    return primelist


#This Function reads the whole binary file and call the helper function for passing block to helper function
def binary_input_read():
    noOfBlocks = 1
    maxSizePerBlock = 4096
    while True:
        data = file_open_pointer.read(maxSizePerBlock)
        if not data:
            break

        Thirdquery(helper_sub_function(data),noOfBlocks)
        noOfBlocks += 1
        data=data[maxSizePerBlock:]



def Thirdquery(blocksDataFeed,blockNumber):
    for i in blocksDataFeed:
        search_birthdate = str(date(i[0].birth_year, i[0].birth_month, i[0].birth_date)).replace('-', '')
        d = str(blockNumber)
        if search_birthdate in Query4_DBM_Index:
            u = Query4_DBM_Index[search_birthdate]
            i = str(u.decode('utf-8')) + "," + str(blockNumber) + str('-') + str(i[1])
            Query4_DBM_Index[search_birthdate] = i
        else:
            Query4_DBM_Index[search_birthdate] = str(blockNumber) + str('-') + str(i[1])


main()
binary_input_read()
file_open_pointer.close()