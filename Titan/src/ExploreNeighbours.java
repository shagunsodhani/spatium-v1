import java.util.Iterator;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Query.Compare;

public class ExploreNeighbours extends Thread{
	
	public TitanGraph graph;
	public Vertex vertex;
	public double original_distance, new_distance;
	
	public ExploreNeighbours(TitanGraph graph, Vertex vertex, double distance){
		this.graph = graph;
		this.vertex = vertex;
		this.original_distance = distance;
		this.new_distance = 0.0;
	}
	
	public ExploreNeighbours(TitanGraph graph, Vertex vertex, double distance1, double distance2){
		this.graph = graph;
		this.vertex = vertex;
		this.original_distance = distance1;
		this.new_distance = distance2;
	}
	
	@Override
	public void run() {
		int count = 0;
		Geoshape pointGeoshape = vertex.getProperty("place");
		double latitude = pointGeoshape.getPoint().getLatitude();
		double longitude = pointGeoshape.getPoint().getLongitude();
		String type = vertex.getProperty("type");
		
		if(original_distance==new_distance){
			for(Iterator<Vertex>iterator2 = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, original_distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator(); iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				count++;
			}
		}else{
			for(Iterator<Vertex>iterator2 = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, new_distance)).has("place", Geo.DISJOINT, Geoshape.circle(latitude, longitude, original_distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator(); iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				count++;
			}
		}		
//		System.out.println("Count = "+count);
		Graph.countMap.put(vertex.getId().toString(), count);
	}

}
