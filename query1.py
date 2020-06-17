import struct
import collections
from datetime import date

file_open_pointer = open("small.bin",'rb')
recordformat = '20s20s70s40s80s25s3i12s25s50s50s'
recordsize = struct.calcsize(recordformat)


#This function reads the each block and return the combined results having 10 records
def helper_sub_function(data):
    sizePerBlock = recordsize
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

#This Function reads the whole binary file and call the helper function for passing block to helper function
def binary_input_read():
    noOfBlocks = 0
    maxSizePerBlock = 4096
    while True:
        data = file_open_pointer.read(maxSizePerBlock)
        if not data:
            break
        noOfBlocks += 1
        first_query(helper_sub_function(data),noOfBlocks)
        data = data[maxSizePerBlock:]

#This function print records whose birthdate is less than 21 years
def first_query(blocksDataFeed,blockNumber):
    Age_Restriction = 21
    for iteration in blocksDataFeed:
        days_in_year = 365.2425
        birthDate = date(iteration.birth_year, iteration.birth_month, iteration.birth_date)
        age = int((date.today() - birthDate).days / days_in_year)
        if(age < 21):
            print("SSN : ",iteration.ssn,"First Name : ",iteration.firstName,"  Last Name: ",iteration.lastName)

def main():
    binary_input_read()

if __name__ == '__main__':
    main()

file_open_pointer.close()