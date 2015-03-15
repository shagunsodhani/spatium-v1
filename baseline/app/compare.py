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

