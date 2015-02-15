import java.util.Iterator;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;

import com.esotericsoftware.minlog.Log.Logger;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Query.Compare;


public class WorkThread extends Thread{
	
	public TitanGraph graph;
	public Geoshape southWest;
	public Geoshape northEast;
	public String type;
	
	public WorkThread(TitanGraph graph, Geoshape southWest, Geoshape northEast, String type) {
		// TODO Auto-generated constructor stub
		this.graph =graph;
		this.southWest = southWest;
		this.northEast = northEast;
		this.type = type;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		int count = 0;
		double southWest_lat = southWest.getPoint().getLatitude();
		double southWest_long = southWest.getPoint().getLongitude();
		double norhtEast_lat = northEast.getPoint().getLatitude();
		double northEast_long = northEast.getPoint().getLongitude();
		Geoshape temp;
		Vertex vertex = null;
		for(Iterator<Vertex>iterator  = graph.query().has("place", Geo.WITHIN, Geoshape.box(southWest_lat, southWest_long, norhtEast_lat, northEast_long)).has("type",Compare.EQUAL, type).vertices().iterator()
				;	iterator.hasNext();){
			vertex = iterator.next();
			count++;
		}
		if(count!=0){
			Geoshape point = vertex.getProperty("place");
			Visualization.typeDist.put(point, count);
		}
//		long threadId = Thread.currentThread().getId();
//        System.out.println("Thread # " + threadId + " is doing this task");
	}

}
