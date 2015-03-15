try:
	import database.mysql as db
except ImportError as exc:
	print("Error: failed to import settings module ({})".format(exc))

class Compare():
	"""Class to compare results from proposed algorithm with results from baseline algorithm for correctness"""	

	# def __init__(self, dbname, sql_populate = "SELECT id, primary_type, latitude, longitude FROM dataset ORDER BY date ASC LIMIT 0, 5000", , threshold_distance=0.2, minPrevalance = 0.001, create_table = 0, kmax = 4, quiet = 0):
	def __init__(self, app_name = "spatium", dbname = "spatium", verbose = 0):	
		self.dbname = dbname
		self.app_name = app_name
		self.conn = db.connect(self.app_name, self.dbname)
		self.cursor = self.conn.cursor()
		self.verbose = verbose

	def compare_results_size2(self, label = 1, prosposed_results_file = "graph.txt"):
		"""Compare results for size 2 from proposed algorithm and baseline method for correctness label is the `label` corresponding to size 2 """
		D = {}
		sql_baseline = "SELECT instanceid1, instanceid2 FROM instance2 WHERE label = "+str(label)
		result = db.read(sql_baseline, self.cursor)
		for i in result:
			id1 = str(i[0])
			id2 = str(i[1])
			if(int(id1) > int(id2)):
				key = id1+":"+id2
			else:
				key = id2+":"+id1
			D[key] = "baseline"

		with open(prosposed_results_file) as graph:
			for line in graph:
				a = line.strip().split()
				id1 = str(a[0])
				id2 = str(a[1])
				if(int(id1) > int(id2)):
					key = id1+":"+id2
				else:
					key = id2+":"+id1
				if key not in D:
					D[key] = "proposed"
				else:
					D[key] = "match"

		match_count = 0
		proposed_count = 0
		baseline_count = 0
		for i in D:
			if D[i] == "match":
				match_count+=1
			elif D[i] == "proposed":
				proposed_count+=1
			else:
				baseline_count+=1

		print "baseline_count", baseline_count
		print "proposed_count", proposed_count
		print "match_count", match_count