from miner import Miner
from graf import Graf
import time
from db import create_db
from db import delete_db

schema = open('schema/spatium.sql','r')
sql = schema.read()
r_values = [1000,2000]
pi_values = [0.3, 0.6, 0.8]
count=1
r_values = [10000]
pi_values = [0.01]
for R in r_values:
    for pi in pi_values:

        dbname = "spatiumbeta"+str(count)
        create_db(dbname, sql)
        start_time = time.time()
        mapping = "Input_Preprocessing/mapping.json"
        inputfile = "Input_Preprocessing/input_preprocessed.json"
        m = Miner(create_table = 1, minPrevalance = pi, threshold_distance = R, mappingFile = mapping, inFile = inputfile, quiet = 1, dbname = dbname)
        msg = "Time taken for R = "+str(R)+", PI = "+str(pi)+", k = "
        m.initialise()
        time_taken = []
        time_taken.append(str(time.time() - start_time))
        # print msg+"1 = "+time_taken
        m.colocation_2()
        time_taken.append(str(time.time() - start_time))
        # print msg+"2 = "+time_taken
        m.colocation_k(3)
        time_taken.append(str(time.time() - start_time))
        # print msg+"3 = "+time_taken
        
        g = Graf(lat = "41.838915902", lng = "-87.72820175", dbname = dbname)
        # g.bootstrap_demo()
        # g.plot_points()
        # g.footer_demo()
        # html_string = g.html
        # file_name = "html/"+str(count)+".html"
        # f = open(file_name,'w')
        # f.write(html_string)
        # g.html = ""
        g.bootstrap_demo()
        g.plot_colocations(k=2)
        g.body_static(R = 1000, PI = 0.1, K = 2)
        print g.html
        delete_db(dbname)
        g.plot_points()