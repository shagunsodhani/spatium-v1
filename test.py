import time

try:
    from database.mysql import create_db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from app.miner import Miner
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

schema = open('schema/spatium.sql','r')
sql = schema.read()

count = 1
for i in range(1,5):
    dbname = "spatium"+str(count)
    count+=1
    create_db(dbname, sql)
    start_time = time.time()
    mapping = "Input_Preprocessing/mapping"+str(i)+".json"
    inputfile = "Input_Preprocessing/input_preprocessed"+str(i)+".json"
    # mapping = "Input_Preprocessing/mapping".json"
    # inputfile = "Input_Preprocessing/input_preprocessed.json"
    a = Miner(create_table = 1, minPrevalance = 0.2, threshold_distance = 5000,mappingFile = mapping, inFile = inputfile, quiet = 1, dbname = dbname)
    a.initialise()
    a.colocation_2()
    a.colocation_k(3)
    time_taken = str(time.time() - start_time)
    print "Time taken for ", i
    print time_taken + " seconds " 
    # del a
