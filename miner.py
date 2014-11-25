import sys
import db
import json
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser


class Miner(object):
	"""Class to implement Co-location Miner"""	


	def __init__(self, mappingFile = "Input_Preprocessing/mapping.json", inFile = "Input_Preprocessing/input_preprocessed.json", app_name = "spatium", threshold_distance=1000, minPrevalance = 0.1, create_table=0):
		
		self.inFile = inFile
		self.mappingFile = mappingFile
		self.mapping = {}
		self.instance_superset = {}
		self.candidate_sizeone = {}
		self.conn = db.connect(app_name)
		self.cursor = self.conn.cursor()
		self.threshold_distance = threshold_distance
		self.minPrevalance = minPrevalance
		self.create = create_table
		# self.initialise()
		self.colocation_2()

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

		k=2
		table_name = "instance"+str(k)
		if self.create == 1: 
			table_sql = "CREATE TABLE IF NOT EXISTS "
			table_sql+="`"+table_name+"` (`label` int(11) NOT NULL,"
			for i in range (0,k):
				table_sql+="`instanceid"+str(i+1)+"` int(11) NOT NULL,"
			
			table_sql += "KEY `label` (`label`) ) "
			db.add_table(table_sql, self.cursor)
		sql = "select type, count(*) from location group by type"
		result = db.read(sql, self.cursor)
		type_count = {}
		candidate_list = []
		for i in result:
			candidate_list.append(int(i[0]))
			type_count[int(i[0])] = int(i[1])

		sql = "select colocation, label from candidate where size = 1"
		result = db.read(sql, self.cursor)
		colocation_label = {}
		for i in result:
			colocation_label[int(i[0])] = int(i[1])

		candidate_list.sort()

		length = len(candidate_list)
		R = self.threshold_distance
		A_temp = {}
		B_temp = {}
		count_a = 0
		count_b = 0  
		for i in range(0, length-1):
			for j in range(i+1, length):
				sql = "select i.instanceid, j.instanceid from location i, location j where i.type = " + str(candidate_list[i]) + " and j.type = "+ str(candidate_list[j])\
				   + " and pow(i.x-j.x, 2) + pow(i.y-j.y, 2) <= "+str(R*R)
				result = db.read(sql, self.cursor)	
				A_temp = {}
				B_temp = {}
				count_a = 0
				count_b = 0
				for l in result:
					a = str(l[0])
					b = str(l[1])
					if a not in A_temp:
						count_a+=1
						A_temp[a] = 0
					if b not in B_temp:
						count_b+=1

				participationIndex = 1.0*(count_a * count_b)
				participationIndex = participationIndex / (type_count[candidate_list[i]] * type_count[candidate_list[j]])		
				if( participationIndex >= self.minPrevalance):
					colocation_temp = str(candidate_list[i])+"|"+str(candidate_list[j])
					# print colocation_temp
					sql_candidate = "INSERT INTO candidate (colocation, pi, size) VALUES ("+colocation_temp+","+str(participationIndex)+", 2)"
					# print sql_candidate
					db.write(sql_candidate, self.cursor, self.conn)
					sql_label = "SELECT max(label) FROM candidate"
					# print sql_label
					label_result = db.read(sql_label, self.cursor)
					# print label_result
					label = label_result[0][0]
					# print label, "label"
					sql_instance = "INSERT INTO "+table_name+" VALUES "
					count = 1
					for l in result:
						a = str(l[0])
						b = str(l[1])
						sql_instance+="("+a+","+b+"),"
						count+=1
						if(count%5000 == 0):
							if(sql_instance[-1]==','):
								sql_instance = sql_instance[:-1]
							db.write(sql_instance, self.cursor, self.conn)
							sql_instance = "INSERT INTO "+table_name+" VALUES "
							print count, "Items inserted into "+table_name+" table"
					if(sql_instance[-1]==','):
						sql_instance=sql_instance[:-1]
					if(sql_instance[-1] == ')'):
						db.write(sql_instance, self.cursor, self.conn)

a = Miner()
