import os
import json

f_output1 = open("mapping.json",'w')
f_output2 = open("input_preprocessed.json",'w')

f_input = open("../data/input/Crimes_2013.json",'r')

d = json.load(f_input)
d_mapping = {}
d_preprocessed = {}

count = 0
for x in d["data"]:

	if (x[23] != None and x[24] != None):
		d_preprocessed[x[8]] = {}
		d_preprocessed[x[8]]["x_coordinate"] = x[23]
		d_preprocessed[x[8]]["y_coordinate"] = x[24]
		d_preprocessed[x[8]]["type"] = x[13]

		if x[13] not in d_mapping:
			d_mapping[x[13]] = count
			count += 1

json.dump(d_mapping,f_output1)
f_output1.close()

json.dump(d_preprocessed,f_output2)
f_output2.close()

print "Total distinct features "+str(count)+'\n'