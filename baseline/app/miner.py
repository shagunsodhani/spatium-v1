import sys
import os
import json
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if not path in sys.path:
    sys.path.insert(1, path)
del path

try:
    import database.mysql as db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from helper import create_table, subset
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


class Miner(object):
	"""Class to implement Co-location Miner"""	


	def __init__(self, dbname, sql_populate = "SELECT id, primary_type, latitude, longitude FROM dataset ORDER BY date ASC LIMIT 0, 5000", app_name = "spatium", threshold_distance=0.2, minPrevalance = 0.001, create_table = 0, kmax = 4, quiet = 0):
		
		self.mapping = {}
		self.instance_superset = {}
		self.app_name = app_name
		self.sql_populate = sql_populate
		self.conn = db.connect(self.app_name, dbname)
		self.cursor = self.conn.cursor()
		self.threshold_distance = threshold_distance
		self.minPrevalance = minPrevalance
		self.create = create_table
		self.quiet = quiet

	def clean(self):
		db.truncate('location', self.cursor)
		db.truncate('instance', self.cursor)
		db.truncate('candidate', self.cursor)
		k=self.kmax
		for i in range(1,k+1):
			table_name = "instance"+str(i)
			db.drop(table_name, self.cursor)

	def initialise(self):
		"""To initialise the class variables"""
		self.initialise_location()
		self.initialise_candidate()
		self.initialise_instance()
	
	def initialise_location(self):
		"""To initialise location table"""
		conn = db.connect(self.app_name, 'spatium')
		cursor = conn.cursor()
		sql = "SELECT DISTINCT(primary_type) FROM dataset"
		result = db.read(sql, cursor)
		j = 1;
		for i in result:
			self.mapping[str(i[0])] = j
			j+=1

		result = db.read(self.sql_populate, cursor)
		cursor.close()
		conn.close()
		for i in result:
			self.instance_superset[i[0]] = {}
			self.instance_superset[i[0]]['type'] = str(self.mapping[str(i[1])])
			self.instance_superset[i[0]]['latitude'] = str(i[2])
			self.instance_superset[i[0]]['longitude'] = str(i[3])

		sql_location = "INSERT INTO location (instanceid, type, lat, lng) values "
		count = 1
		for i in self.instance_superset:
			sql_location +="("+str(i)+","+str(self.instance_superset[i]['type']) + "," + str(self.instance_superset[i]['latitude'])+","+str(self.instance_superset[i]['longitude']) + "),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_location[-1]==','):
					sql_location=sql_location[:-1]
				db.write(sql_location, self.cursor, self.conn)
				sql_location = "INSERT INTO location (instanceid, type, lat, lng) values "
				if(self.quiet==0):
					print count, "Items inserted into location table"
		if(sql_location[-1]==','):
			sql_location=sql_location[:-1]
		if(sql_location[-1] == ')'):
			db.write(sql_location, self.cursor, self.conn)

	def initialise_candidate(self):
		"""To initialise candidate table"""
		sql_candidate = "INSERT INTO candidate (colocation, pi) values "
		count = 1
		for i in self.mapping :
			sql_candidate += "("+str(self.mapping[i])+",1),"
			count = (count+1)
			if(count%5000 == 0):
				if(sql_candidate[-1]==','):
					sql_candidate=sql_candidate[:-1]
				# print sql_candidate
				db.write(sql_candidate, self.cursor, self.conn)
				sql_candidate = "INSERT INTO candidate (colocation, pi) values "
				if(self.quiet==0):
					print count, "Items inserted into candidate table"		
		if(sql_candidate[-1]==','):
			sql_candidate=sql_candidate[:-1]
		if(sql_candidate[-1]==')'):
			# print sql_candidate
			db.write(sql_candidate, self.cursor, self.conn)		

	def initialise_instance(self):
		"""To initialise instance table"""

		sql = "select colocation, label from candidate where size = 1"
		result = db.read(sql, self.cursor)
		label_colocation = {}
		for i in result:
			label_colocation[str(i[0])] = str(i[1])
		sql_instance = "INSERT INTO instance (label, instance) values "
		count = 1
		for i in self.instance_superset:
			sql_instance +="("+str(label_colocation[str(self.instance_superset[i]['type'])])+","+str(i)+"),"
			count = (count+1)
			if(count%1000 == 0):
				if(sql_instance[-1]==','):
					sql_instance=sql_instance[:-1]
				db.write(sql_instance, self.cursor, self.conn)
				if(self.quiet==0):
					print count, "Items inserted into instance table"
				sql_instance = "INSERT INTO instance (label, instance) values "
		if(sql_instance[-1]==','):
			sql_instance=sql_instance[:-1]
		if(sql_instance[-1] == ')'):
			db.write(sql_instance, self.cursor, self.conn)
	
	def colocation_2(self):

		"""to generate colocations of size 2"""

		k=2
		table_candidate_name = "candidate"+str(k)
		table_instance_name = "instance"+str(k)

		if self.create == 1:
			create_table(k, self.cursor)

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
				   + " and ACOS(SIN(PI()*i.lat/180.0)*SIN(PI()*j.lat/180.0)+COS(PI()*i.lat/180.0)*COS(PI()*j.lat/180.0)*COS(PI()*j.lng/180.0-PI()*i.lng/180.0))*6371 <= "+str(R)
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
						B_temp[b] = 0

				participationIndex = 1.0*(count_a * count_b)
				participationIndex = participationIndex / (type_count[candidate_list[i]] * type_count[candidate_list[j]])		
				if( participationIndex >= self.minPrevalance):

					participationIndex = round(participationIndex,7)
					sql_candidate = "INSERT INTO "+table_candidate_name+" (typeid1, typeid2, pi, labelprev1, labelprev2) \
									VALUES ("+str(candidate_list[i])+","+str(candidate_list[j])+","+str(participationIndex)+","+str(i)+","+str(j)+")"
					db.write(sql_candidate, self.cursor, self.conn)

					sql_label = "SELECT max(label) FROM "+str(table_candidate_name)
					
					label_result = db.read(sql_label, self.cursor)
					# print label_result
					label = str(label_result[0][0])

					sql_instance = "INSERT INTO "+table_instance_name+" VALUES "
					count = 1
					for l in result:
						a = str(l[0])
						b = str(l[1])
						sql_instance+="("+label+","+a+","+b+"),"
						count+=1
						if(count%5000 == 0):
							if(sql_instance[-1]==','):
								sql_instance = sql_instance[:-1]
							# print sql_instance
							# print "precise"
							db.write(sql_instance, self.cursor, self.conn)
							sql_instance = "INSERT INTO "+table_instance_name+" VALUES "
							if(self.quiet==0):
								print count, "Items inserted into "+table_instance_name+" table"
					if(sql_instance[-1]==','):
						sql_instance=sql_instance[:-1]
					if(sql_instance[-1] == ')'):
						# print sql_instance
						db.write(sql_instance, self.cursor, self.conn)
					
	def colocation_k(self, k):

		"""colocation for size k using colocation for size k-1"""
		table_candidate_name_old = "candidate"+str(k-1)
		table_instance_name_old = "instance"+str(k-1)

		table_candidate_name = "candidate"+str(k)
		table_instance_name = "instance"+str(k)

		C_k_1 = {}
		accept = [] 
		#contains all candidate co-locations for table instance computation

		if self.create == 1:
			create_table(k, self.cursor)
		
		sql = "SELECT "
		for i in range(1, k):
			a = "typeid"+str(i)
			sql+=a+","
		sql=sql[:-1]
		sql+=" FROM "+table_candidate_name_old			
		# print sql
		result = db.read(sql,self.cursor)
		for i in result:
			a = ""
			for j in i:
				a+=str(j)+"|" 
			a=a[:-1]
			C_k_1[a] = 0

		# print C_k_1

		sql = "SELECT "
		for i in range(1, k):
			a = "typeid"+str(i)
			sql+="C1."+a+","
		sql+="C2.typeid"+str(k-1)+",C1.label, C2.label"
		sql+=" FROM "+str(table_candidate_name_old)+" C1, "+str(table_candidate_name_old)+" C2 WHERE "
		for i in range(1,k-1):
			a = "typeid"+str(i)
			sql+="C1."+a+" = C2."+a+" AND "
		sql+="C1.typeid"+str(k-1)+" < C2.typeid"+str(k-1)
		# print sql

		result = db.read(sql,self.cursor)
		for i in result:
			a = []
			for j in range(0,k):
				a.append(str(i[j]))
			b = subset(a)
			flag = 1
			for j in b:
				if(j not in C_k_1):
					flag = 0
					break
			if flag == 1:
				accept.append(i)

		R = self.threshold_distance
		# R = 1000000
		for i in accept:
			labelprev1 = str(i[k])
			labelprev2 = str(i[k+1])
			sql = "SELECT "
			for l in range(1, k):
				a = "T1.instanceid"+str(l)
				sql+=a+","
			sql+="T2.instanceid"+str(k-1)	
			sql+=" FROM "+table_instance_name_old+" T1, "+table_instance_name_old + " T2, location L1, location L2 WHERE T1.label = "\
				+labelprev1 + " AND T2.label = "+labelprev2 + " AND "
			for j in range(1,k-1):	
				sql+= "T1.instanceid"+str(j) + " = T2.instanceid"+str(j)+" AND "
			sql+= " L1.instanceid = T1.instanceid"+str(k-1)+" AND L2.instanceid = T2.instanceid"+str(k-1)+" \
			AND ACOS(SIN(PI()*L1.lat/180.0)*SIN(PI()*L2.lat/180.0)+COS(PI()*L1.lat/180.0)*COS(PI()*L2.lat/180.0)*COS(PI()*L2.lng/180.0-PI()*L1.lng/180.0))*6371 <= "+str(R)
			if(self.quiet == 0):
				print sql
			instance_result = db.read(sql, self.cursor)
			# print instance_result
			k_temp = {}
			count_k = {}
			for l in instance_result:
				length = len(l)
				for m in range(0,length):
					if m not in count_k:
						count_k[m] = 0
						k_temp[m] = {}
					if l[m] not in k_temp[m]:
						count_k[m]+=1
						k_temp[m][l[m]] = 0
			
			participationIndex = 1.0
			sql = "select type, count(*) from location group by type"
			result_count = db.read(sql, self.cursor)
			type_count = {}
			for m in result_count:
				type_count[int(m[0])] = int(m[1])

			for m in range(0,k):
				participationIndex *= (count_k[m])
				participationIndex = participationIndex / (type_count[int(i[m])])		
			
			if( participationIndex >= self.minPrevalance):

				participationIndex = round(participationIndex,7)
				sql_candidate = "INSERT INTO "+table_candidate_name+" ("
				temp_sql = ""
				for m in range(1,k+1):
					sql_candidate+="typeid"+str(m)+","
					temp_sql +=str(i[m-1])+","
				sql_candidate+=" pi, labelprev1, labelprev2) VALUES ("
				sql_candidate+=temp_sql
				sql_candidate+=str(participationIndex)+","+labelprev1 +","+labelprev2+")"
				# print sql_candidate

				db.write(sql_candidate, self.cursor, self.conn)

				sql_label = "SELECT max(label) FROM "+str(table_candidate_name)
				
				label_result = db.read(sql_label, self.cursor)
				label = str(label_result[0][0])

				sql_instance = "INSERT INTO "+table_instance_name+" VALUES "
				count = 1
				for l in instance_result:
					sql_instance+="("+label+","
					for m in range(0,k):
						sql_instance+=str(l[m])+","
					sql_instance = sql_instance[:-1]
					sql_instance+="),"
					count+=1
					if(count%5000 == 0):
						if(sql_instance[-1]==','):
							sql_instance = sql_instance[:-1]
						# print sql_instance
						# print "precise"
						db.write(sql_instance, self.cursor, self.conn)
						sql_instance = "INSERT INTO "+table_instance_name+" VALUES "
						if(self.quiet==0):
							print count, "Items inserted into "+table_instance_name+" table"
				if(sql_instance[-1]==','):
					sql_instance=sql_instance[:-1]
				if(sql_instance[-1] == ')'):
					# print sql_instance
					db.write(sql_instance, self.cursor, self.conn)
