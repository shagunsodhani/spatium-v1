import json
f = open("input_preprocessed.json",'r')
d = json.load(f)

d_grp = {}
for x in d:
	if d[x]['type'] not in d_grp:
		d_grp[d[x]['type']] = {}
		d_grp[d[x]['type']][x] = 0
	else:
		d_grp[d[x]['type']][x] = 0

for x in d_grp:
	print x,len(d_grp[x])
