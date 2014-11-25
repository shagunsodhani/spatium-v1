import sys
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
		self.initialise_location()
		self.initialise_candidate()
		self.initialise_instance()
	
	def initialise_location(self):
		f_infile = open(self.inFile, 'r')
		self.instance_superset = json.load(f_infile)
		sql_location = "INSERT INTO location (instanceid, x, y, type) values "
		count = 1
		for i in self.instance_superset:
			sql_location +="("+str(i)+","+str(self.instance_superset[i]['x_coordinate'])+","+str(self.instance_superset[i]['y_coordinate'])+","+str(self.instance_superset[i]['type'])+"),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_location[-1]==','):
					sql_location=sql_location[:-1]
				db.write(sql_location, self.cursor, self.conn)
				sql_location = "INSERT INTO location (instanceid, x, y, type) values "
				print count, "Items inserted into location table"
		if(sql_location[-1]==','):
			sql_location=sql_location[:-1]
		if(sql_location[-1] == ')'):
			db.write(sql_location, self.cursor, self.conn)

	def initialise_candidate(self):
		f_mapping = open(self.mappingFile, 'r')
		self.mapping = json.load(f_mapping)
		sql_candidate = "INSERT INTO candidate (colocation, pi) values "
		count = 1
		for i in self.mapping :
			sql_candidate += "("+str(self.mapping[i])+",1),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_candidate[-1]==','):
					sql_candidate=sql_candidate[:-1]
				print sql_candidate
				db.write(sql_candidate, self.cursor, self.conn)
				sql_candidate = "INSERT INTO candidate (colocation, pi) values "
				print count, "Items inserted into candidate table"		
		if(sql_candidate[-1]==','):
			sql_candidate=sql_candidate[:-1]
		if(sql_candidate[-1]==')'):
			print sql_candidate
			db.write(sql_candidate, self.cursor, self.conn)		

	def initialise_instance(self):

		sql = "select colocation, label from candidate where size = 1"
		result = db.read(sql, self.cursor)
		label_colocation = {}
		for i in result:
			label_colocation[str(i[0])] = str(i[1])

		f_infile = open(self.inFile, 'r')
		self.instance_superset = json.load(f_infile)
		sql_instance = "INSERT INTO instance (label, instance) values "
		count = 1
		for i in self.instance_superset:
			sql_instance +="("+str(label_colocation[str(self.instance_superset[i]['type'])])+","+str(i)+"),"
			count = (count+1)
			if(count%1000 == 0):
				if(sql_instance[-1]==','):
					sql_instance=sql_instance[:-1]
				db.write(sql_instance, self.cursor, self.conn)
				print count, "Items inserted into instance table"
				sql_instance = "INSERT INTO instance (label, instance) values "
		if(sql_instance[-1]==','):
			sql_instance=sql_instance[:-1]
		if(sql_instance[-1] == ')'):
			db.write(sql_instance, self.cursor, self.conn)
				

a = Miner()
