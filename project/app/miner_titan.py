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
    import database.titan as db
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))

try:
    from helper import create_table, subset
except ImportError as exc:
    print("Error: failed to import settings module ({})".format(exc))


class Miner(object):
	'''Class to implement Co-location Miner'''


	def __init__(self, mappingFile = "../Data/mapping.json", inFile = "../Data/input_preprocessed.json", app_name = "spatium_titan", threshold_distance=1000, minPrevalance = 0.001, kmax = 4, quiet = 0):
		
		self.inFile = inFile
		self.mappingFile = mappingFile
		# self.mapping = {}
		# print dbname
		self.g = db.connect(app_name)
		self.threshold_distance = threshold_distance
		self.minPrevalance = minPrevalance
		self.quiet = quiet

	def insert_vertices_from_file(self):
		'''To initialise location table'''
		f_mapping = open(self.mappingFile, 'r')
		mapping = json.load(f_mapping)
		count = 1
		for i in mapping :
			self.g.vertices.create(id = i,latitude = mapping[i]['latitude'], type = mapping[i]['type'], longitude = mapping[i]['longitude'])
			count = (count+1)
		print count, "number of vertices inserted"
		print self.g.V