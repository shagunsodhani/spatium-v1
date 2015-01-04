import time

try:
    from database.mysql import create_db, delete_db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from app.miner import Miner
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from app.graf import Graf
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

schema = open('schema/spatium.sql','r')
sql = schema.read()
r_values = [1000,10000]
pi_values = [0.01, 0.5]
count=0
j = -1
# r_values = [10000]
# pi_values = [0.01]
file_list = [1,4,7,10]

for R in r_values:
    for pi in pi_values:
        count+=1
        dbname = "spatiumbeta"+str(count)
        for k in range(1,2):
            j+=1
            
            
            # create_db(dbname, sql)
            # start_time = time.time()
            # mapping = "Input_Preprocessing/mapping.json"
            # inputfile = "Input_Preprocessing/input_preprocessed.json"
            # m = Miner(create_table = 1, minPrevalance = pi, threshold_distance = R, mappingFile = mapping, inFile = inputfile, quiet = 1, dbname = dbname)
            # msg = "Time taken for R = "+str(R)+", PI = "+str(pi)+", k = "
            # m.initialise()
            # # time_taken = []
            # time_taken = str(time.time() - start_time)
            # start_time = time.time()
            # print msg+"1 = "+time_taken
            # m.colocation_2()
            # ttime_taken = str(time.time() - start_time)
            # start_time = time.time()
            # print msg+"2 = "+time_taken
            # m.colocation_k(3)
            # time_taken = str(time.time() - start_time)
            # start_time = time.time()
            # print msg+"3 = "+time_taken
            
            g = Graf(lat = "41.838915902", lng = "-87.72820175", dbname = dbname)
            # g.bootstrap_demo()
            # g.plot_points()
            # g.footer_demo()
            # html_string = g.html
            file_name = "html/"+str(file_list[j])+".html"
            f = open(file_name,'w')
            
            # g.html = ""
            g.bootstrap_demo()
            f.write(g.html)
            # print "shagun"
            # g.html =""
            g.plot_points()
            f.write(g.html)
            # print "deathofme"
            # g.html =""
            g.body_static(R = R, PI = pi, K = k)
            f.write(g.html)
            f.close()
            # print "death"
            # g.html =""
            # delete_db(dbname)
            # g.plot_points()