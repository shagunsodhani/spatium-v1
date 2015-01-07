import sys

try:
    import database.mysql as db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


def create_table(k, cursor):

    """create table for storing co-locations and instances of given size"""
    table_candidate_name = "candidate"+str(k)
    table_instance_name = "instance"+str(k)

    table_instance_sql = "CREATE TABLE IF NOT EXISTS "
    table_instance_sql+="`"+table_instance_name+"` (`label` int(11) NOT NULL,"
    for i in range (0,k):
        table_instance_sql+="`instanceid"+str(i+1)+"` int(11) NOT NULL,"
    table_instance_sql += "KEY `label` (`label`) ) "
    # print table_instance_sql
    db.add_table(table_instance_sql, cursor)
    
    table_candidate_sql = "CREATE TABLE IF NOT EXISTS "
    table_candidate_sql+="`"+table_candidate_name+"` (`label` int(11) NOT NULL AUTO_INCREMENT,"
    for i in range (0,k):
        table_candidate_sql+="`typeid"+str(i+1)+"` int(11) NOT NULL,"    
    table_candidate_sql += "`pi` double NOT NULL, `labelprev1` int(11) NOT NULL, `labelprev2` int(11) NOT NULL, KEY `label` (`label`) ) "
    # print table_candidate_sql
    db.add_table(table_candidate_sql, cursor)

def subset(a):
    result = []
    length = len(a)
    for i in range(0,length):
        temp = ""
        for j in a[:i]:
            temp+=str(j)+"|"
        for j in a[i+1:]:
            temp+=str(j)+"|"
        temp=temp[:-1]            
        result.append(temp)
    return result

