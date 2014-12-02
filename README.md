spatium
=======

Spatial Co-location Rule 

Co-location Miner Algorithm Implementation proposed by [1]


[1] [Discovering Spatial Co-location Patterns : A Summary of Results](http://www.spatial.cs.umn.edu/paper_ps/sstd01.pdf) 
- for K = 2 no need of Apriori Function
- for K > 2 first generate candidate co-locations using apriori property and then find prevalent co-locations using join and pruning using min_prev

Next task is to port Join Based Algo on Graph Database (Titan) 

We have use real world crime data of Chicago city, USA from [10] for our experiments. The data consists of crime incidents with details including location information. The idea is to predict which types of crime are likely to co-occur based on the past history of crime incidents at various locations in the city. 

[Initial Survey Report] (https://sites.google.com/site/sanketmehtaiitr/spatium)
[Demo] (http://shagunsodhani.in/spatium)
[Presentation] (http://slides.com/shagun/spatium)
