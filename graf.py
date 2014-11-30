import db
import json
from ConfigParser import ConfigParser
import os

#---------------------------------------------------------Read Config-------------------------------------------------------------------------#



config=ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config', 'config.cfg'))


class graf(object):
	"""Class to generate html file that would generate the map using google maps api"""	

	def __init__(self, dbname = "spatium", inFile = "Input_Preprocessing/plot_on_map.json", app_name = "spatium", lat = 0, lng = 0, zoom = 8):
		
		self.inFile = inFile
		self.key = config.get(app_name, "map_key")
		self.conn = db.connect(app_name, dbname)
		self.cursor = self.conn.cursor()
		self.html = ''
		self.lat = lat
		self.lng = lng
		self.zoom = 8
		self.markers = []
		self.mapping = {}
		# self.data = {}

	def plot_points(self):
		"""To generate the html code for plotting points on the map"""
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
        	count = 0
        	for i in data:
        		self.html+="var myLatLng = new google.maps.LatLng("+str(data[i]['latitude'])+", "+str(data[i]['longitude'])+");\n\
	          		    var beachMarker = new google.maps.Marker({\n\
	          		    position: myLatLng,\n\
				    map: map,\n\
				    icon: {\n"

			marker_type = data[i]['type'] 
			if marker_type not in self.mapping:
				self.mapping[marker_type] = self.markers[count]
				count+=1
				    
			self.html+="path: "+str(self.mapping[marker_type])+",\n\
      				    scale: 4\n\
    					},\n\
    				    draggable: true,\n\
	         		    });\n"
		
		self.html+="}"

		self.footer()
		print self.html

	def plot_colocations(self, k):
		"""To generate the html code for plotting colocations"""
		self.bootstrap()

		sql = "SELECT "
		for i in range(1,k+1):
			sql+="l"+str(i)+".lat, l"+str(i)+".lng, "
		sql=sql[:-2]
		sql+=" FROM instance"+str(k)+", "
		sql_b = " WHERE "
		for i in range(1,k+1):
			sql+="location l"+str(i)+", "
			sql_b+="l"+str(i)+".instanceid = instance3.instanceid"+str(i)+" AND " 
		sql=sql[:-2]
		sql+=sql_b
		sql=sql[:-4]
		
		result = db.read(sql, self.cursor)
		result = []
		self.html+="function initialize() \n\
			    {\n\
		            	var mapOptions = \n\
		            	{\n\
		            	center: { lat: "+str(self.lat)+", lng: "+str(self.lng)+"},\n\
			        	zoom: "+str(self.zoom)+"\n\
        				};\n\
        			var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);\n\
				var bermudaTriangle;\n"
		for i in result:
			a = ''
			for j in range (0,k):
				a+="new google.maps.LatLng("+str(i[j][0])+", "+str(i[j][1])+",\n"
			a = a.strip('\n')
			a = a[:-1]
			# print a

			self.html+="var triangleCoords = ["+a+"];\n\
			      bermudaTriangle = new google.maps.Polygon({\n\
				paths: triangleCoords,\n\
			        strokeColor: '#FF0000',\n\
			        strokeOpacity: 0.8,\n\
			        strokeWeight: 2,\n\
			        fillColor: '#FF0000',\n\
			        fillOpacity: 0.35\n\
			      });\n\
			      bermudaTriangle.setMap(map);\n"
		
		self.html+"}\n"
		self.footer()
		print self.html
		# print sql

	def initialise(self):
		"""To initialise variables by reading from the json file"""
		prefix = "google.maps.SymbolPath."
		a = prefix+"CIRCLE"
		self.markers.append(a)
		a = prefix+"FORWARD_OPEN_ARROW"
		self.markers.append(a)
		a = prefix+"BACKWARD_OPEN_ARROW"
		self.markers.append(a)
		a = prefix+"FORWARD_CLOSED_ARROW"
		self.markers.append(a)
		a = prefix+"BACKWARD_CLOSED_ARROW"
		self.markers.append(a)
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
	    			 <script type=\"text/javascript\" src=\"https://maps.googleapis.com/maps/api/js?key="+self.key+"\"></script>\n\
	    			 <script type=\"text/javascript\">\n"

	def footer(self):
		"""To add the footer"""
		self.html+="google.maps.event.addDomListener(window, 'load', initialize);\n\
			    </script>\n</head>\n\
		            <body>\n\
		            <div id=\"map-canvas\"></div>\n\
		            </body>\n\
		            </html>\n"


a = graf(lat = "41.838915902", lng = "-87.72820175", dbname = "spatium_I3")
a.plot_colocations(k=3)
