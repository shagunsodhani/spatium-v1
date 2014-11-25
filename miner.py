import sys
import helper
import db
import json
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser


class Miner(object):
	"""Class to implement Co-location Miner"""	


	def __init__(self, mappingFile = "Input_Preprocessing/mapping.json", inFile = "Input_Preprocessing/input_preprocessed.json", app_name = "spatium", minSupport = 0.15 , minConfidence = 0.6):
		
		self.inFile = inFile
		self.mappingFile = mappingFile
		self.mapping = {}
		self.instance_superset = {}
		self.conn = db.connect(app_name)
		self.cursor = conn.cursor()
		initialise()
    
    def initialise(self):
    	"""To initialise the class variables"""
    	self.mapping = json.load(self.mappingFile)
    	self.instance_superset = json.load(self.inFile)
    	for i in self.instance_superset:
    		print i

filename = "INTEGRATED-DATASET.csv"
inFile = helper.dataFromFile(filename)
a = Apriori(inFile)
a.runApriori()
a.printResults()
