try:
	import database.mysql as db
except ImportError as exc:
	print("Error: failed to import settings module ({})".format(exc))

class Generate():
	"""Class to compare results from proposed algorithm with results from baseline algorithm for correctness"""	

	def __init__(self, app_name = "spatium", dbname = "spatium", verbose = 0):	
		self.dbname = dbname
		self.app_name = app_name
		self.conn = db.connect(self.app_name, self.dbname)
		self.cursor = self.conn.cursor()
		self.verbose = verbose
