import java.util.Iterator;

import org.elasticsearch.common.unit.DistanceUnit.Distance;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Query.Compare;


public class ExploreNeighbours extends Thread{
	
	public TitanGraph graph;
	public Vertex vertex;
	public double distance;
	
	public ExploreNeighbours(TitanGraph graph, Vertex vertex, double distance){
		// TODO Auto-generated constructor stub
		this.graph = graph;
		this.vertex = vertex;
		this.distance = distance;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		int count = 0;
		Geoshape pointGeoshape = vertex.getProperty("place");
		double latitude = pointGeoshape.getPoint().getLatitude();
		double longitude = pointGeoshape.getPoint().getLongitude();
		String type = vertex.getProperty("type");
		
		for(Iterator<Vertex>iterator2  = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator()
				;	iterator2.hasNext();){
			Vertex vertex2 = iterator2.next();
			count++;
		}
//		System.out.println("Count = "+count);
		Graph.countMap.put(vertex.getId().toString(), count);
	}

}
