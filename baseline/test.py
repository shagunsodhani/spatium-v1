import time

try:
    from database.mysql import create_db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from app.miner import Miner
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

schema = open('../schema/spatium.sql','r')
sql = schema.read()

count = 0
dbname = "spatium"+str(count)
create_db(dbname, sql)
start_time = time.time()
sql_populate = "SELECT id, primary_type, latitude, longitude FROM dataset ORDER BY date ASC LIMIT 0, 10000"
a = Miner(create_table = 1, minPrevalance = 0.05, threshold_distance = 0.3, quiet = 1, dbname = dbname, sql_populate = sql_populate)
a.initialise()
time_taken = str(time.time() - start_time)
print "Time taken for initialising "
print time_taken + " seconds "
# print a.explore_neighbours()
start_time = time.time()
a.colocation_2(type_of_pi=2)
time_taken = str(time.time() - start_time)
print "Time taken for size 2"
print time_taken + " seconds "
start_time = time.time()
a.colocation_k(3, type_of_pi=2)
time_taken = str(time.time() - start_time)
print "Time taken for size 3"
print time_taken + " seconds "
start_time = time.time()
a.colocation_k(4, type_of_pi=2)
time_taken = str(time.time() - start_time)
print "Time taken for size 4"
print time_taken + " seconds "
start_time = time.time()
a.colocation_k(5, type_of_pi=2)
time_taken = str(time.time() - start_time)
print "Time taken for size 5"
print time_taken + " seconds "

to_delete = int(raw_input('Enter 1 to clean the database else enter 0\n'))

if (to_delete):
    a.clean()
