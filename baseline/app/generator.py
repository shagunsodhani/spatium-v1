try:
	import database.mysql as db
except ImportError as exc:
	print("Error: failed to import settings module ({})".format(exc))

class Generate():
	"""Class to compare results from proposed algorithm with results from baseline algorithm for correctness"""	

	def __init__(self, app_name = "spatium", old_dbname = "spatium", new_dbname = "spatium_new", verbose = 0, create_new_db = 0):	
		self.old_dbname = old_dbname
		self.new_dbname = new_dbname
		self.app_name = app_name
		self.old_conn = db.connect(self.app_name, self.old_dbname)
		self.old_cursor = self.old_conn.cursor()
		self.new_conn = db.connect(self.app_name, self.new_dbname)
		self.new_cursor = self.new_conn.cursor()
		self.verbose = verbose
		self.create_new_db = create_new_db

	def gen(number_of_types = 3, r1 = 0.2, r2 = 1.0, upper_limit = 1000):
		"""This method generates a smaller dataset from the given, larger raw dataset.""" 

		sql = "SELECT primary_type, count(*) as count FROM dataset group by primary_type order by count DESC"
		result = db.read(sql, self.old_cursor)
		type_mapping = {}
		sorted_type = []
		k = number_of_types
		for i in result : 
			type_mapping[str(i[0])] = int(i[1])
			sorted_type.append(str(i[0]))

		if self.verbose:
			print type_mapping
			print sorted_type

		selected_type = []
		for i in range(0, k):
			selected_type.append(sorted_type[i])

		if self.verbose:
			print selected_type

		test_type = selected_type[1]
		r = str(r1)
		
		sql = "SELECT latitude, longitude FROM dataset WHERE primary_type = \'"+selected_type[0]+ "\' ORDER BY id desc LIMIT 0,1"
		result = mysql.read(sql, old_cursor)
		latitude =  str(result[0][0]) 
		longitude = str(result[0][1])

		limit = "LIMIT 0, "+str(upper_limit)
		sql = "SELECT id,`latitude` as lat, `longitude` as lng, primary_type, date FROM `dataset` WHERE ACOS(SIN(PI()*latitude/180.0)*SIN(PI()*"+latitude+"/180.0)+COS(PI()*latitude/180.0)*COS(PI()*"+latitude+"/180.0)*COS(PI()*"+longitude+"/180.0-PI()*longitude/180.0))*6378.137 < "+r+" AND latitude!=0.0 AND longitude!=0.0 AND `primary_type` = \'" + test_type + "\' "+limit
		print sql

		if self.verbose:
			print sql
		result = db.read(sql, self.old_cursor)
		sql = "INSERT INTO dataset values "
		for i in result:
			temp_sql =  ""
			temp_sql+=str(i[0])+", "+str(i[1])+", "+str(i[2])+", \'"+str(i[3])+"\' "+", "+str(i[4])
			temp_sql = temp_sql[:-2]
			sql+="("+temp_sql+"), "
		sql = sql[:-2]
		db.write(sql, self.new_cursor, self.new_conn)

		if self.verbose:
			print sql

		r = str(0.2)
		test_type = selected_type[0]
		limit = "LIMIT 0, "+str(upper_limit)

		sql = "SELECT id,`latitude` as lat, `longitude` as lng, primary_type, date FROM `dataset` WHERE ACOS(SIN(PI()*latitude/180.0)*SIN(PI()*"+latitude+"/180.0)+COS(PI()*latitude/180.0)*COS(PI()*"+latitude+"/180.0)*COS(PI()*"+longitude+"/180.0-PI()*longitude/180.0))*6378.137 < "+r+" AND latitude!=0.0 AND longitude!=0.0 AND `primary_type` = \'" + test_type + "\' "+limit
		print sql

		if self.verbose:
			print sql

		result = db.read(sql, self.old_cursor)
		sql = "INSERT INTO dataset values "
		for i in result:
			temp_sql =  ""
			temp_sql+=str(i[0])+", "+str(i[1])+", "+str(i[2])+", \'"+str(i[3])+"\' "+", "+str(i[4])
			temp_sql = temp_sql[:-2]
			sql+="("+temp_sql+"), "
		sql = sql[:-2]
		db.write(sql, self.new_cursor, self.new_conn)

		if self.verbose:
			print sql


		r = str(r2)
		test_type = selected_type[0]
		sql = "SELECT latitude, longitude FROM dataset WHERE ACOS(SIN(PI()*latitude/180.0)*SIN(PI()*"+latitude+"/180.0)+COS(PI()*latitude/180.0)*COS(PI()*"+latitude+"/180.0)*COS(PI()*"+longitude+"/180.0-PI()*longitude/180.0))*6378.137 > "+r+" AND `primary_type` = \'" + selected_type[0] + "\' LIMIT 0,1"
		result = mysql.read(sql, old_cursor)
		latitude =  str(result[0][0]) 
		longitude = str(result[0][1])

		r = str(r1)
		limit = "LIMIT 0, "+str(upper_limit)

		sql = "SELECT id,`latitude` as lat, `longitude` as lng, primary_type, date FROM `dataset` WHERE ACOS(SIN(PI()*latitude/180.0)*SIN(PI()*"+latitude+"/180.0)+COS(PI()*latitude/180.0)*COS(PI()*"+latitude+"/180.0)*COS(PI()*"+longitude+"/180.0-PI()*longitude/180.0))*6378.137 < "+r+" AND latitude!=0.0 AND longitude!=0.0 AND `primary_type` = \'" + test_type + "\' "+limit
		if verbose:
			print sql
		result = db.read(sql, self.old_cursor)
		sql = "INSERT INTO dataset values "

		for i in result:
			temp_sql =  ""
			temp_sql+=str(i[0])+", "+str(i[1])+", "+str(i[2])+", \'"+str(i[3])+"\' "+", "+str(i[4])
			temp_sql = temp_sql[:-2]
			sql+="("+temp_sql+"), "
		sql = sql[:-2]
		db.write(sql, self.new_cursor, self.new_conn)

		if self.verbose:
			print sql

		test_type = selected_type[2]
		limit = "LIMIT 0, "+str(upper_limit)

		sql = "SELECT id,`latitude` as lat, `longitude` as lng, primary_type, date FROM `dataset` WHERE ACOS(SIN(PI()*latitude/180.0)*SIN(PI()*"+latitude+"/180.0)+COS(PI()*latitude/180.0)*COS(PI()*"+latitude+"/180.0)*COS(PI()*"+longitude+"/180.0-PI()*longitude/180.0))*6378.137 < "+r+" AND latitude!=0.0 AND longitude!=0.0 AND `primary_type` = \'" + test_type + "\' "+limit
		if verbose:
			print sql
		result = db.read(sql, self.old_cursor)
		sql = "INSERT INTO dataset values "

		for i in result:
			temp_sql =  ""
			temp_sql+=str(i[0])+", "+str(i[1])+", "+str(i[2])+", \'"+str(i[3])+"\' "+", "+str(i[4])
			temp_sql = temp_sql[:-2]
			sql+="("+temp_sql+"), "

		sql = sql[:-2]
		db.write(sql, self.new_cursor, self.new_conn)
