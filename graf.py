import db
import json
from ConfigParser import ConfigParser
import os

#---------------------------------------------------------Read Config-------------------------------------------------------------------------#



config=ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config', 'config.cfg'))


class graf(object):
	"""Class to generate html file that would generate the map using google maps api"""	

	def __init__(self, inFile = "Input_Preprocessing/plot_on_map.json", app_name = "spatium", lat = 0, lng = 0):
		
		self.inFile = inFile
		self.key = config.get(app_name, "map_key")
		self.html = ''
		self.lat = lat
		self.lng = lng
		# self.data = {}

	def gen_html(self):
		"""To generate the html code"""
		data = self.initialise()
		self.bootstrap()
		self.html+="<script type=\"text/javascript\">\n\
					function initialize() \n\
					{\n\
		            	var mapOptions = \n\
		            	{\n\
		            	center: { lat: "+str(self.lat)+", lng: "+str(self.lng)+"},\n\
			        	zoom: "+str(self.zoom)+"\n\
        				};\n\
        				var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);\n"
        	# print data
        	for i in data:
        		self.html+="var myLatLng = new google.maps.LatLng("+str(data[i]['latitude'])+", "+str(data[i]['longitude'])+");\n\
	          		    var beachMarker = new google.maps.Marker({\n\
	          		    position: myLatLng,\n\
				    map: map,\n\
	         		    });\n"
		
		self.html+="}\ngoogle.maps.event.addDomListener(window, 'load', initialize);\n\
				    </script>\n"

		self.footer()
		print self.html

	def initialise(self):
		"""To initialise variables by reading from the json file"""
		data = open(self.inFile, 'r')
		return json.load(data)

	def bootstrap(self):
		"""To initialise html code"""
		self.html = "<!DOCTYPE html>\n\
					 <html>\n\
		  			 <head>\n\
		  			 <style type=\"text/css\">\n\
		  			 html, body, #map-canvas { height: 100%; margin: 0; padding: 0;}\n\
    				 </style>\n\
	    			 <script type=\"text/javascript\" src=\"https://maps.googleapis.com/maps/api/js?key="+self.key+"\"></script>\n"

	def footer(self):
		"""To add the footer"""
		self.html+="</head>\n\
		            <body>\n\
		            <div id=\"map-canvas\"></div>\n\
		            </body>\n\
		            </html>\n"

a = graf(lat = "41.838915902", lng = "-87.72820175")
a.gen_html()
