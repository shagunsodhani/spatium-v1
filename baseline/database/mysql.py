#! /usr/bin/python
#---------------------------------------------------------Import Modules----------------------------------------------------------------------#

import os
from ConfigParser import ConfigParser
import math

try:
    import MySQLdb
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


def connect(app_name = "spatium", db_name = 1, config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../config', 'config.cfg') ):

    '''Open database connection and return conn object to perform database queries'''
    config=ConfigParser()
    config.read(config_path)
    host=config.get(app_name,"host")
    user=config.get(app_name,"user")
    passwd=config.get(app_name,"passwd")

    if(db_name == 1):
        db=config.get(app_name,"db")
    else:
        db = db_name
    charset=config.get(app_name,"charset")
    use_unicode=config.get(app_name,"use_unicode")

    try:
        if (db_name==-1):
            conn=MySQLdb.connect(host,user,passwd,charset=charset,use_unicode=use_unicode)
        else:
            conn=MySQLdb.connect(host,user,passwd,db,charset=charset,use_unicode=use_unicode)
        return conn
    except MySQLdb.Error, e:
        print "ERROR %d IN CONNECTION: %s" % (e.args[0], e.args[1])
        return 0


def write(sql,cursor,conn):
    '''Perform insert and update operations on the databse.
       Need to pass the cursor object as a parameter'''
    try:
        cursor.execute(sql)
        conn.commit()
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN WRITE OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql


def read(sql,cursor):
    '''Perform read operations on the databse.
       Need to pass the cursor object as a parameter'''
    try:
        cursor.execute(sql)
        result=cursor.fetchall()
        return result
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN READ OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql


def truncate(table_name, cursor):
    sql = "TRUNCATE TABLE " + str(table_name)
    read(sql, cursor)


def drop(table_name, cursor):
    sql = "DROP TABLE IF EXISTS " + str(table_name)
    print sql
    read(sql, cursor)


def check_column(table,column,cursor):
    '''Used to check if `column` exists in `table`
       Need to pass the cursor object as a parameter'''
    sql="SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' AND COLUMN_NAME =  '{}'".format(table,column)
    try:
        return cursor.execute(sql)
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN CHECK COLUMN OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql

def add_column(sql,cursor):
    '''Used to add columns into tables'''
    try:
        cursor.execute(sql)
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN ADD COLUMN OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql

def add_table(sql,cursor):
    '''Used to create a new table in the db'''
    try:
        cursor.execute(sql)
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN ADD TABLE OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql

def create_db(db_name, sql, app_name = "spatium"):
    '''Create Database'''

    conn = connect(app_name, -1)
    sql_db = "CREATE DATABASE IF NOT EXISTS "+db_name
    cursor = conn.cursor()
    try:
        cursor.execute(sql_db)
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN CREATE DB OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql
    
    cursor.close()
    conn.close()
    conn = connect(app_name, db_name)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN CREATE DB OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql

    # cursor.close()
    # conn.close()    

def delete_db(db_name, app_name = "spatium"):
    '''Delete Database'''

    conn = connect(app_name, -1)
    sql = "DROP DATABASE "+db_name
    print sql
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN DELETE DB OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql
    cursor.close()
    conn.close()