from miner import Miner
import time
from db import create_db

file_list = []
schema = open('schema/spatium.sql','r')
sql = schema.read()

count = 1
for i in file_list:
    sql_db = "CREATE DATABASE spatium"+str(count)
    count+=1
    create_db(sql_db, sql)
    start_time = time.time()
    mapping = "Input_Preprocessing/"+str(i[1])
    inputfile = "Input_Preprocessing/"+str(i[0])
    a = Miner(create_table = 1, minPrevalance = 0.2, threshold_distance = 5000,mappingFile = mapping, inFile = inputfile)
    a.initialise()
    a.colocation_2()
    a.colocation_k(3)
    print "Time taken for ", i
    print ("%s seconds ---" % time.time() - start_time)
    del a
