import db
import json
from ConfigParser import ConfigParser
import os

#---------------------------------------------------------Read Config-------------------------------------------------------------------------#



config=ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config', 'config.cfg'))


class Graf(object):
	"""Class to generate html file that would generate the map using google maps api"""

	def __init__(self, dbname = "spatium", inFile = "Input_Preprocessing/plot_on_map.json", app_name = "spatium", lat = 0, lng = 0, zoom = 8, icon_size = 24):

		self.inFile = inFile
		self.key = config.get(app_name, "map_key")
		self.conn = db.connect(app_name, dbname)
		self.cursor = self.conn.cursor()
		self.html = ''
		self.lat = lat
		self.lng = lng
		self.zoom = 8
		self.icon_size = icon_size
		self.mapping = {}
		self.color_mapping = {}
		self.color_mapping[1] = "#FF0000"
		self.color_mapping[2] = "#0404B4"
		self.color_mapping[3] = "#088A08"
		# self.data = {}

	def plot_points(self):
		"""To generate the html code for plotting points on the map"""
		data = self.initialise()
		# self.bootstrap()
		self.html+="function initialize() \n\
					{\n\
		            	var mapOptions = \n\
		            	{\n\
		            	center: { lat: "+str(self.lat)+", lng: "+str(self.lng)+"},\n\
			        	zoom: "+str(self.zoom)+"\n\
        				};\n\
        				var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);\n"
		count = 1
		for i in data:
			marker_type = data[i]['type'] 
			if marker_type not in self.mapping:
				self.mapping[marker_type] = "assets/map/pins/"+str(self.icon_size)+"/pin"+str(count)+".png"
				count+=1

			self.html+="var image = \'"+str(self.mapping[marker_type])+"\';\n\
	               		    var myLatLng = new google.maps.LatLng("+str(data[i]['latitude'])+", "+str(data[i]['longitude'])+");\n\
	          		    var beachMarker = new google.maps.Marker({\n\
	          		    position: myLatLng,\n\
				    map: map,\n\
				    icon: image,\n\
    				    draggable: true,\n\
	         		    });\n"

			
		self.html+="}"

		# self.footer()
		# print self.html

	def plot_colocations(self, k):
		"""To generate the html code for plotting colocations"""
		# self.bootstrap_demo()

		sql = "SELECT "
		for i in range(1,k+1):
			sql+="l"+str(i)+".lat, l"+str(i)+".lng, "
		# sql=sql[:-2]
		sql+="label FROM instance"+str(k)+", "
		sql_b = " WHERE "
		for i in range(1,k+1):
			sql+="location l"+str(i)+", "
			sql_b+="l"+str(i)+".instanceid = instance"+str(k)+".instanceid"+str(i)+" AND " 
		sql=sql[:-2]
		sql+=sql_b
		sql=sql[:-4]

		# print sql
		
		result = db.read(sql, self.cursor)
		
		# print result
		self.html+="function initialize() \n\
			    {\n\
		            	var mapOptions = \n\
		            	{\n\
		            	center: { lat: "+str(self.lat)+", lng: "+str(self.lng)+"},\n\
			        	zoom: "+str(self.zoom)+"\n\
        				};\n\
        			var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);\n\
				var precise;\n"
		if result:
			for i in result:
				a = ''
				for j in range (0,2*k-1,2):
					a+="new google.maps.LatLng("+str(i[j])+", "+str(i[j+1])+"),\n"
				a = a.strip('\n')
				a = a[:-1]
					# print a

				self.html+="var triangleCoords = ["+a+"];\n\
					      precise = new google.maps.Polygon({\n\
						paths: triangleCoords,\n\
					        strokeColor: '"+str(self.color_mapping[i[-1]])+"',\n\
					        strokeOpacity: 0.8,\n\
					        strokeWeight: 2,\n\
					        fillColor: '"+str(self.color_mapping[i[-1]])+"',\n\
					        fillOpacity: 0.2\n\
					      });\n\
					      precise.setMap(map);\n"
		self.html+="}\n"

	def initialise(self):
		"""To initialise variables by reading from the json file"""
		data = open(self.inFile, 'r')
		return json.load(data)

	def bootstrap_default(self):
		"""To initialise default html code"""
		self.html= "<!DOCTYPE html>\n\
					 <html>\n\
		  			 <head>\n\
		  			 <style type=\"text/css\">\n\
		  			 html, body, #map-canvas { height: 100%; margin: 0; padding: 0;}\n\
    				 </style>\n\
	    			 <script type=\"text/javascript\" src=\"https://maps.googleapis.com/maps/api/js?key="+self.key+"\"></script>\n\
	    			 <script type=\"text/javascript\">\n"

	def bootstrap_demo(self):
		"""To add the bootstrap code for demo html files"""
		self.html="<!DOCTYPE html>\n\
					<html lang=\"en\">\n\
					<head>\n\
					<meta charset=\"utf-8\">\n\
					<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n\
					<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
					<title>Spatium</title>\n\
				    <link href=\"bootstrap/css/bootstrap.min.css\" rel=\"stylesheet\">\n\
				    <link href=\"bootstrap/css/spatium.css\" rel=\"stylesheet\">\n\
	    			<style type=\"text/css\">\n\
      				html, body, .container-fluid, .row, .main, #map-canvas { height: 100%; margin-left: 0; padding-left: 5%;}\n\
    				</style>\n\
    				<script type=\"text/javascript\"\n\
      				src=\"https://maps.googleapis.com/maps/api/js?key="+str(self.key)+"\"></script>\n\
    				<script type=\"text/javascript\">\n"

	def body_demo(self):
		"""To add the footer code for demo html files"""
		self.html="google.maps.event.addDomListener(window, 'load', initialize);\n\
			    </script>\n</head>\n\
    				<body>\n\
    				<nav class=\"navbar navbar-inverse navbar-fixed-top\" role=\"navigation\">\n\
	      				<div class=\"container-fluid\">\n\
	        				<div class=\"navbar-header\">\n\
		          				<button type=\"button\" class=\"navbar-toggle collapsed\" data-toggle=\"collapse\" data-target=\"#navbar\" aria-expanded=\"false\" aria-controls=\"navbar\">\n\
			            			<span class=\"sr-only\">Toggle navigation</span>\n\
			            			<span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						        </button>\n\
					          	<a class=\"navbar-brand\" href=\"#\">Spatium</a>\n\
	        				</div>\n\
	        				<div class=\"collapse navbar-collapse\" id=\"bs-example-navbar-collapse-1\">\n\
	      						<ul class=\"nav navbar-nav navbar-right\">\n\							        <li class=\"dropdown\">\n\
								        <a href=\"#\" class=\"dropdown-toggle\" data-toggle=\"dropdown\" role=\"button\" aria-expanded=\"false\">Threshold Distance<span class=\"caret\"></span></a>\n\
								        <ul class=\"dropdown-menu\" role=\"menu\">\n\
								        	<li><a href=\"#\">1</a></li>\n\
								            <li><a href=\"#\">2</a></li>\n\
								            <li><a href=\"#\">3</a></li>\n\
								        </ul>\n\
							    	</li>\n\
						        	<li class=\"dropdown\">\n\
		          						<a href=\"#\" class=\"dropdown-toggle\" data-toggle=\"dropdown\" role=\"button\" aria-expanded=\"false\">Participation Index <span class=\"caret\"></span></a>\n\
		          						<ul class=\"dropdown-menu\" role=\"menu\">\n\
		            						<li><a href=\"#\">1</a></li>\n\
								            <li><a href=\"#\">2</a></li>\n\
								            <li><a href=\"#\">3</a></li>\n\
								        </ul>\n\
								    </li>\n\
							        <li class=\"dropdown\">\n\
								        <a href=\"#\" class=\"dropdown-toggle\" data-toggle=\"dropdown\" role=\"button\" aria-expanded=\"false\">Size of Co-location <span class=\"caret\"></span></a>\n\
								       	<ul class=\"dropdown-menu\" role=\"menu\">\n\
								           	<li><a href=\"#\">1</a></li>\n\
								           	<li><a href=\"#\">2</a></li>\n\
								          	<li><a href=\"#\">3</a></li>\n\
								       	</ul>\n\
							        </li>\n\
		      					</ul>\n\
						    </div>\n\
						</nav>\n\
    				<div class=\"container-fluid\">\n\
				    	<div class=\"row\">\n\
				        	<div class=\"col-sm-3 col-md-2 sidebar\">\n\
				          		<ul class=\"nav nav-sidebar\">\n\
						            <li class=\"active\"><a href=\"#\">Demo <span class=\"sr-only\">(current)</span></a></li>\n\
						            <li><a href=\"#\">Dataset</a></li>\n\
						            <li><a href=\"#\">Analytics</a></li>\n\
						            <li><a href=\"#\">Team</a></li>\n\
				          		</ul>\n\
	        				</div>\n\
	        				<div class=\"col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main\">\n\
	          					<h1 class=\"page-header\">Map</h1>\n\
	          					<div class=\"row placeholders\">\n\
	            					<div class=\"col-sm-12\" id=\"map-canvas\" >\n\
	              						<h4>Label</h4>\n\
	              						<span class=\"text-muted\">Something else</span>\n\
	            					</div>\n\
	          					</div>\n\
	          					<h2 class=\"sub-header\">Section title</h2>\n\
	          						<div class=\"table-responsive\">\n\
	            						<table class=\"table table-striped\">\n\
	              							<thead>\n\
	                							<tr>\n\
	                  								<th>#</th>\n\
	                  								<th>Header</th>\n\
	                  								<th>Header</th>\n\
									                <th>Header</th>\n\
									                <th>Header</th>\n\
									            </tr>\n\
									        </thead>\n\
									        <tbody>\n\
									            <tr>\n\
										            <td>1,001</td>\n\
										            <td>Lorem</td>\n\
										            <td>ipsum</td>\n\
										            <td>dolor</td>\n\
										            <td>sit</td>\n\
									        	</tr>\n\
									        </tbody>\n\
									    </table>\n\
									</div>\n\
								</div>\n\
							</div>\n\
						</div>\n\
					</div>\n\
					<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>\n\
				    <script src=\"bootstrap/js/bootstrap.min.js\"></script>\n\
				  	</body>\n\
					</html>\n"

	def body_static(self, R, PI, K):
		"""To add the body code for static files"""
		self.html="google.maps.event.addDomListener(window, 'load', initialize);\n\
			    </script>\n</head>\n\
    				<body>\n\
    				<nav class=\"navbar navbar-inverse navbar-fixed-top\" role=\"navigation\">\n\
	      				<div class=\"container-fluid\">\n\
	        				<div class=\"navbar-header\">\n\
		          				<button type=\"button\" class=\"navbar-toggle collapsed\" data-toggle=\"collapse\" data-target=\"#navbar\" aria-expanded=\"false\" aria-controls=\"navbar\">\n\
			            			<span class=\"sr-only\">Toggle navigation</span>\n\
			            			<span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						        </button>\n\
					          	<a class=\"navbar-brand\" href=\"#\">Spatium</a>\n\
	        				</div>\n\
	        				<div class=\"collapse navbar-collapse navbar-right\" id=\"bs-example-navbar-collapse-1\">\n\
	      						<button type=\"button\" class=\"navbar-toggle collapsed\" data-toggle=\"collapse\" data-target=\"#navbar\" aria-expanded=\"false\" aria-controls=\"navbar\">\n\
			            			<span class=\"sr-only\">Toggle navigation</span>\n\
			            			<span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						            <span class=\"icon-bar\"></span>\n\
						        </button>\n\
					          	<a class=\"navbar-brand\" href=\"https://github.com/shagunsodhani/spatium\">Github</a>\n\
						    </div>\n\
						</nav>\n\
    				<div class=\"container-fluid\">\n\
				    	<div class=\"row\">\n\
				        	<div class=\"col-sm-3 col-md-2 sidebar\">\n\
				          		<ul class=\"nav nav-sidebar\">\n\
						        	<li><a href=\"1.html\">R = 1000, PI = 0.1, K = 1</a></li>\n\
						            <li><a href=\"2.html\">R = 1000, PI = 0.1, K = 2</a></li>\n\
						            <li><a href=\"3.html\">R = 1000, PI = 0.1, K = 3</a></li>\n\
						            <li><a href=\"4.html\">R = 1000, PI = 0.6, K = 1</a></li>\n\
						            <li><a href=\"5.html\">R = 1000, PI = 0.6, K = 2</a></li>\n\
						            <li><a href=\"6.html\">R = 1000, PI = 0.6, K = 3</a></li>\n\
						            <li><a href=\"7.html\">R = 2000, PI = 0.1, K = 1</a></li>\n\
						            <li><a href=\"8.html\">R = 2000, PI = 0.1, K = 2</a></li>\n\
						            <li><a href=\"9.html\">R = 2000, PI = 0.1, K = 3</a></li>\n\
						            <li><a href=\"10.html\">R = 2000, PI = 0.6, K = 1</a></li>\n\
						            <li><a href=\"11.html\">R = 2000, PI = 0.6, K = 2</a></li>\n\
						            <li><a href=\"12.html\">R = 2000, PI = 0.6, K = 3</a></li>\n\
				          		</ul>\n\
	        				</div>\n\
	        				<div class=\"col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main\">\n\
	          					<h4 class=\"page-header\">Threshold Distance(R) = "+str(R)+", Participation Index(PI) = "+str(PI)+", Size of Colocation(K) = "+str(K)+"</h4>\n\
	          					<div class=\"row placeholders\">\n\
	            					<div class=\"col-sm-12\" id=\"map-canvas\" >\n\
	              						<h4>Label</h4>\n\
	              						<span class=\"text-muted\">Something else</span>\n\
	            					</div>\n\
	          					</div>\n\
	          					<h2 class=\"sub-header\">Section title</h2>\n\
	          						<div class=\"table-responsive\">\n\
	            						<table class=\"table table-striped\">\n\
	              							<thead>\n\
	                							<tr>\n\
	                  								<th>Colocation Between</th>\n\
	                  								<th>Number of Instances</th>\n\
	                  								<th>Participation Index</th>\n\
									            </tr>\n\
									        </thead>\n\
									        <tbody>\n\
									            <tr>\n\
										            <td>A,B,C</td>\n\
										            <td>1000</td>\n\
										            <td>0.4</td>\n\
									        	</tr>\n\
									        </tbody>\n\
									    </table>\n\
									</div>\n\
								</div>\n\
							</div>\n\
						</div>\n\
					</div>\n\
					<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>\n\
				    <script src=\"bootstrap/js/bootstrap.min.js\"></script>\n\
				  	</body>\n\
					</html>\n"

	def body_default(self):
		"""To add the footer"""
		self.html="google.maps.event.addDomListener(window, 'load', initialize);\n\
			    </script>\n</head>\n\
		            <body>\n\
		            <div id=\"map-canvas\"></div>\n\
		            </body>\n\
		            </html>\n"
