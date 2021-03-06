import os
import json
import time
import datetime

f_output1 = open("mapping.json",'w')
f_output2 = open("input_preprocessed.json",'w')

f_input = open("../data/input/Crimes_2013.json",'r')

d = json.load(f_input)
d_mapping = {}
d_preprocessed = {}

count = 1
startDate = "2013-06-01"
endDate = "2013-06-02"
startTime = time.mktime(datetime.datetime.strptime(startDate,"%Y-%m-%d").timetuple())
endTime = time.mktime(datetime.datetime.strptime(endDate,"%Y-%m-%d").timetuple())

d_type = {}

for x in d["data"]:
	if 'None' not in str(x[26]): 
		temp = str(x[26]).split('T')
		
		s = temp[0]

		tempTime = time.mktime(datetime.datetime.strptime(s,"%Y-%m-%d").timetuple())
		if (x[23] != None and x[24] != None and (tempTime >= startTime) and (tempTime <= endTime) and (x[13]=="ASSAULT" or x[13]=="BATTERY" or x[13] =="THEFT")):
			#print s,temp
			if (x[13] not in d_type):
				d_type[x[13]] = 0
				d_mapping[x[13]] = count
				count += 1

			if d_type[x[13]] < 50:
				d_preprocessed[x[8]] = {}
				d_preprocessed[x[8]]["type"] = x[13]
				d_preprocessed[x[8]]["x_coordinate"] = x[23]
				d_preprocessed[x[8]]["y_coordinate"] = x[24]
				d_preprocessed[x[8]]["latitude"] = x[27]
				d_preprocessed[x[8]]["longitude"] = x[28]
				d_type[x[13]] += 1

for x in d_preprocessed:
	temp = d_preprocessed[x]["type"]
	d_preprocessed[x]["type"] = str(d_mapping[temp])

json.dump(d_mapping,f_output1)
f_output1.close()

json.dump(d_preprocessed,f_output2)
f_output2.close()

print "Done"

