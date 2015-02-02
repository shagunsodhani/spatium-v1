spatium
=======

Spatial Co-location Rule

Spatium is a part of our B.Tech Project (Final Year Project) at [Indian Institue of Technology, Roorkee](http://www.iitr.ac.in/).

Our problem statement reads **Mining colocation rules given a spatial dataset using Graph Database**. Over the years numerous approaches have been proposed for this problem albeit with their own limitations. Furthermore major research has been limited to relational databases. We intend to leverage *graph database* to mine these rules more efficiently. Another aspect we would explore is using *MapReduce paradigm*.

To get a better understanding of the problem and understand the bottlenecks in traditional Join-based approach for co-location rule mining, we implemented Co-location rule miner algorithm as proposed by Shashi Shekhar et al. [Discovering Spatial Co-location Patterns : A Summary of Results](http://www.spatial.cs.umn.edu/paper_ps/sstd01.pdf) in python.

Next task is to port the Join-Based Algorithm to a graph database. We plan on using [Titan](http://thinkaurelius.github.io/titan/). 

We are using crime data for City of Chicago, USA. It can be accessed [here](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). 

Stage - I
For more technical details, go through our [Initial Survey Report](https://sites.google.com/site/sanketmehtaiitr/spatium). Also go through our [demo](http://shagunsodhani.in/spatium/demo.html) and [presentation](http://slides.com/shagun/spatium).

Stage - II
We are using [Titan](http://thinkaurelius.github.io/titan/): Distributed Graph Database for our project. We have setup a titan with cassandra as a backend stroage and elasticsearch as external indexing technique. To get better understanding about Titan, we conducted several exteriments, (compared Titan Standard Index with Elasticsearch, implemented multi-threaded version of neighbourhood exploration, etc). For more details, go through our [Titan Setup and its exploration Report](https://sites.google.com/site/sanketmehtaiitr/spatium). 
