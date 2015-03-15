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

	



