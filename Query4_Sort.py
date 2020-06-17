import struct
import collections
import datetime
from operator import attrgetter

Sortinglist = []

file = open("small.bin", 'rb')
sorted_file_new = open("sorted.bin",'wb')
recordformat = '20s20s70s40s80s25s3i12s25s50s50s'
recordsize = struct.calcsize(recordformat)

def helper_sub_function(data):
    sizePerBlock = 405
    primelist=[]
    collectedResult = collections.namedtuple('KeyPersonDetails', 'firstName lastName job company address phone_number birth_date birth_month birth_year ssn username email url')
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

def binary_input_read():
    noOfBlocks = 0
    maxSizePerBlock = 4096
    while True:
        data = file.read(maxSizePerBlock)
        if not data:
            break
        noOfBlocks += 1
        data = helper_sub_function(data)
        for iteration in data:
            Sortinglist.append(iteration)
    return data

def createsorted_binary(data_record):
    s = '\x00'
    record_sorted = sorted(data_record, key=attrgetter('birth_year', 'birth_month', 'birth_date'))
    dataRecordCount = 0
    for record in record_sorted:
        dataRecordCount += 1
        data = struct.pack(recordformat, record[0].encode('utf-8'), record[1].encode('utf-8'),
                        record[2].encode('utf-8'), record[3].encode('utf-8'), record[4].encode('utf-8'), record[5].encode('utf-8'), record[6],
                        record[7], record[8], record[9].encode('utf-8'), record[10].encode('utf-8'), record[11].encode('utf-8'),
                        record[12].encode('utf-8'))
        sorted_file_new.write(data)
        if (dataRecordCount % 10 == 0):
            value = struct.pack('46s',s.encode('utf-8'))
            sorted_file_new.write(value)

def main():
    binary_input_read()
    createsorted_binary(Sortinglist)
    file.close()
    sorted_file_new.close()

if __name__ == '__main__':
    main()
