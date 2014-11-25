import sys
import db
import json
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser


class Miner(object):
	"""Class to implement Co-location Miner"""	


	def __init__(self, mappingFile = "Input_Preprocessing/mapping.json", inFile = "Input_Preprocessing/input_preprocessed.json", app_name = "spatium", threshold_distance=1000):
		
		self.inFile = inFile
		self.mappingFile = mappingFile
		self.mapping = {}
		self.instance_superset = {}
		self.candidate_sizeone = {}
		self.conn = db.connect(app_name)
		self.cursor = self.conn.cursor()
		self.initialise()
		self.colocation_2()
		self.threshold_distance = threshold_distance

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
	
	def colocation_2(self):
		"""to generate colocations of size 2"""
		sql = "select type, count(*) from location"
		result = db.read(sql, self.cursor)
		type_count = {}
		candidate_list = []
		for i in result:
			candidate_list.append(int(i[0]))
			type_count[int(i[0])] = int(i[1])

		sql = "select candidate, label from candidate where size = 1"
		result = db.read(sql, self.cursor)
		candidate_label = {}
		for i in result:
			candidate_label[int(i[0])] = int(i[1])

		candidate_list.sort()

		length = len(candidate_list)

		R = self.threshold_distance
		for i in range(0:length-1):
			for j in range(i+1:length):
			sql = "select * from location i, location j where i.type = " + str(candidate_list[i]) + " and j.type = "+ str(candidate_list[j])\
				   + "and pow(i.x-j.x, 2) + pow(i.y-j.y, 2) <= "+str(R*R)
			result = db.read(sql, self.cursor)	
			for i in result:
				print i
a = Miner()
