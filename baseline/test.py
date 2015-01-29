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
sql_populate = "SELECT id, primary_type, latitude, longitude FROM dataset ORDER BY date ASC LIMIT 0, 50"
a = Miner(create_table = 1, minPrevalance = 0.2, threshold_distance = 0.2, quiet = 1, dbname = dbname, sql_populate = sql_populate)
a.initialise()
a.colocation_2()
# a.colocation_k(3)
time_taken = str(time.time() - start_time)
print "Time taken "
print time_taken + " seconds "

to_delete = int(raw_input('Enter 1 to clean the database else enter 0\n'))

if (to_delete):
    a.clean()
