from miner import Miner
import time
from db import create_db

schema = open('schema/spatium.sql','r')
sql = schema.read()

count = 1
for i in range(1,5):
    dbname = "spatium"+str(count)
    count+=1
    create_db(dbname, sql)
    start_time = time.time()
    mapping = "Input_Preprocessing/mapping"+str(i[1])+".json"
    inputfile = "Input_Preprocessing/input_preprocessed"+str(i[0])+".json"
    a = Miner(create_table = 1, minPrevalance = 0.2, threshold_distance = 5000,mappingFile = mapping, inFile = inputfile, quiet = 1)
    a.initialise()
    a.colocation_2()
    a.colocation_k(3)
    print "Time taken for ", i
    print ("%s seconds ---" % time.time() - start_time)
    del a
