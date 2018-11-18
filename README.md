spatium
=======

Spatial Co-location Pattern Mining using Graph Database

Spatium is the codebase for our [B.Tech Final Year Project](https://arxiv.org/abs/1810.09007) at [Indian Institue of Technology, Roorkee](http://www.iitr.ac.in/).


Spatial co-location pattern mining refers to the task of discovering the group of objects or events that co-occur at many places. Extracting these patterns from spatial data is very difficult due to the complexity of spatial data types, spatial relationships, and spatial auto-correlation. We model the co-location pattern discovery as a clique enumeration problem over a neighborhood graph (which is materialized using a distributed graph database). Further, we propose three new traversal based algorithms, namely `CliqueEnumG`, `CliqueEnumK` and `CliqueExtend`. These algorithms allow for a trade-off between time and memory requirements and support interactive data analysis without having to recompute all the intermediate results.


We used the crime data for City of Chicago, USA. It can be accessed [here](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2). 

We used [Titan](http://thinkaurelius.github.io/titan/) Distributed Graph Database for materializing the graph. We used cassandra as the backend stroage and elasticsearch for external indexing. 

To install the dependencies, run `Titan/install.sh`


If you use our code, please cite our [paper](https://arxiv.org/abs/1810.09007)

```
@ARTICLE{2018arXiv181009007V,
   author = {{Vaibhav Mehta}, S. and {Sodhani}, S. and {Patel}, D.},
    title = "{Spatial Co-location Pattern Mining - A new perspective using Graph Database}",
  journal = {ArXiv e-prints},
archivePrefix = "arXiv",
   eprint = {1810.09007},
 primaryClass = "cs.DB",
 keywords = {Computer Science - Databases, Computer Science - Distributed, Parallel, and Cluster Computing},
     year = 2018,
    month = oct,
   adsurl = {http://adsabs.harvard.edu/abs/2018arXiv181009007V},
  adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
```
