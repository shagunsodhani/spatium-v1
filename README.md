spatium
=======

Spatial Co-location Rule

Spatium is a part of our B.Tech Project (Final Year Project) at [Indian Institue of Technology, Roorkee](http://www.iitr.ac.in/).

Our problem statement reads **Mining colocation rules given a spatial dataset using Graph Database**. Over the years numerous approaches have been proposed for this problem albeit with their own limitations. Furthermore major research has been limited to relational databases. We intend to leverage *graph database* to mine these rules more efficiently. Another aspect we would explore is using *MapReduce paradigm*.

To get a better understanding of the problem and understand the bottlenecks in traditional Join-based approach for co-location rule mining, we implemented Co-location rule miner algorithm as proposed by Shashi Shekhar et al. [Discovering Spatial Co-location Patterns : A Summary of Results](http://www.spatial.cs.umn.edu/paper_ps/sstd01.pdf) in python.

Next task is to port the Join-Based Algorithm to a graph database. We plan on using [Titan](http://thinkaurelius.github.io/titan/). 

We are using crime data for City of Chicago, USA. It can be accessed [here](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). 

For more technical details, go through our [Initial Survey Report](https://sites.google.com/site/sanketmehtaiitr/spatium). Also go through our [demo](http://shagunsodhani.in/spatium/demo.html) and [presentation](http://slides.com/shagun/spatium).
