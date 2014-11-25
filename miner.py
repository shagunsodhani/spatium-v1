import sys
import helper
import db
import json
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser


class Miner(object):
	"""Class to implement Co-location Miner"""	


	def __init__(self, mappingFile = "Input_Preprocessing/mapping.json", inFile = "Input_Preprocessing/input_preprocessed.json", app_name = "spatium", minSupport = 0.15 , minConfidence = 0.6):
		
		self.inFile = inFile
		self.mappingFile = mappingFile
		self.mapping = {}
		self.instance_superset = {}
		self.candidate_sizeone = {}
		self.conn = db.connect(app_name)
		self.cursor = self.conn.cursor()
		self.initialise()

	def initialise(self):
		"""To initialise the class variables"""
		self.mapping = json.load(self.mappingFile)
		self.instance_superset = json.load(self.inFile)
		sql_candidate = "INSERT INTO candidate (colocation, pi) values "
		sql_location = "INSERT INTO location (instanceid, x, y) values "
		count = 1
		for i in self.instance_superset:
			sql_location +="("+str(i)+","+str(self.instance_superset[i['x_location']])+","+str(self.instance_superset[i['y_location']])+"),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_location[-1]==','):
					sql_location=sql_location[:-1]
				db.write(sql_location, self.cursor, self.conn)
				sql_location = "INSERT INTO location (instanceid, x, y) values "
				print count, "Items inserted into location table"
		if(sql_location[-1]==','):
			sql_location=sql_location[:-1]
		db.write(sql_location, self.cursor, self.conn)		

		count = 1
		for i in self.mapping :
			sql_candidate = "("+str(self.mapping[i])+",1),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_candidate[-1]==','):
					sql_candidate=sql_candidate[:-1]
				db.write(sql_candidate, self.cursor, self.conn)
				sql_candidate = "INSERT INTO candidate (colocation, pi) values "
				print count, "Items inserted into candidate table"		
		if(sql_candidate[-1]==','):
			sql_candidate=sql_candidate[:-1]
		db.write(sql_candidate, self.cursor, self.conn)		



a = Miner()
