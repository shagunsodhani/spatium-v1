import os
import json
import time
import datetime

f_output = open("plot_on_map.json",'w')

f_input = open("../data/input/Crimes_2013.json",'r')

d = json.load(f_input)
d_mapping = {}
d_preprocessed = {}

#count = 1
startDate = "2013-06-01"
endDate = "2013-06-02"
startTime = time.mktime(datetime.datetime.strptime(startDate,"%Y-%m-%d").timetuple())
endTime = time.mktime(datetime.datetime.strptime(endDate,"%Y-%m-%d").timetuple())


for x in d["data"]:
	if 'None' not in str(x[26]): 
		temp = str(x[26]).split('T')
		
		s = temp[0]

		tempTime = time.mktime(datetime.datetime.strptime(s,"%Y-%m-%d").timetuple())
		if (x[23] != None and x[24] != None and (tempTime >= startTime) and (tempTime <= endTime) and (x[13]=="ASSAULT" or x[13]=="BATTERY" or x[13] =="CRIMINAL DAMAGE")):
			#print s,temp
			d_preprocessed[x[8]] = {}
			d_preprocessed[x[8]]["type"] = x[13]
			d_preprocessed[x[8]]["latitude"] = x[27]
			d_preprocessed[x[8]]["longitude"] = x[28]
			#d_preprocessed[x[8]]["type"] = x[13]

json.dump(d_preprocessed,f_output)
f_output.close()

print "Done"

