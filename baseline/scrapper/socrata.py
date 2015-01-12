import sys
import os
import time
import datetime
from ConfigParser import ConfigParser

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if not path in sys.path:
    sys.path.insert(1, path)
del path

try:
    import database.mysql as db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    import requests
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


def date_to_timestamp(stime):
	# print stime
	stime = stime.split('T')
	date = stime[0]
	temp = date.split("-")
	a = []
	a.append(int(temp[0]))
	a.append(int(temp[1]))
	a.append(int(temp[2]))
	date = stime[1].split(':')
	for i in date:
		a.append(int(i))
	a = datetime.datetime(a[0], a[1], a[2], a[3], a[4], a[5]).timetuple()
	# year, month, day, hour, minute, second, microsecond, and tzinfo.
	return int(time.mktime(a))

class socrata(object):
	"""Class to fetch data using socrata API"""
	def __init__(self, app_name = "spatium", limit = 10000, config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../config', 'config.cfg')):
		
		self.conn = db.connect(app_name)
		self.cursor = self.conn.cursor()
		self.url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
		self.limit = limit
		config=ConfigParser()
		config.read(config_path)
		self.socrata_key = config.get(app_name, "socrata_key")
	
	def fetch_json(self, offset=0):
		payload = {'$limit': self.limit, '$offset': offset, '$$app_token':self.socrata_key}
		
		try :
			r = requests.get(self.url, params=payload)
		except requests.exceptions.ChunkedEncodingError:
			print payload
			return self.fetch_json(offset = offset)

		to_save = ['latitude', 'longitude', 'id', 'primary_type','date']
		print r.url
		if r.json():
			# print r.json()
			sql = "INSERT INTO dataset ("
			for i in to_save:
				sql+=i+" , "
			sql=sql[:-2]
			sql+= ") VALUES "
			to_save = ['latitude', 'longitude', 'id', 'primary_type','date']
			for i in r.json():
				to_insert = "( "
				for j in to_save:
					if j not in i.keys():
						i[j] = "\'\'"
					else:
						if j == 'date':
							i[j] = str(date_to_timestamp(i[j]))
						i[j] = "\'"+i[j]+"\'"	
					to_insert+=i[j]+", "
				to_insert = to_insert[:-2]
				to_insert+='), '
				sql+=to_insert
			sql = sql[:-2]
			db.write(sql, self.cursor, self.conn)
			return 1
		else:
			return 0

	def fetch_all(self):
		offset = 580000
		while(self.fetch_json(offset = offset)):
			offset+=self.limit
			print offset, " elements inserted in db."


a = socrata(limit = 10000)
a.fetch_all() 

