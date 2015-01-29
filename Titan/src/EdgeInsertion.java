import java.util.Iterator;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Query.Compare;


public class EdgeInsertion extends Thread{
	public TitanGraph graph;
	public Vertex vertex;
	public double distance;
	
	public EdgeInsertion(TitanGraph graph2, Vertex vertex, double distance) {
		this.graph = graph2;
		this.vertex = vertex;
		this.distance = distance;
	}
	
	public void run(){
		Geoshape pointGeoshape = vertex.getProperty("place");
		double latitude = pointGeoshape.getPoint().getLatitude();
		double longitude = pointGeoshape.getPoint().getLongitude();
		long id_vertex1 = Long.parseLong(vertex.getId().toString());
		
		String type = vertex.getProperty("type");
		
		for(Iterator<Vertex>iterator2  = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator()
				;	iterator2.hasNext();){
			Vertex vertex2 = iterator2.next();
			long id_vertex2 = Long.parseLong(vertex2.getId().toString());
			
			if(id_vertex1<id_vertex2){
				Geoshape pointGeoshape2 = vertex2.getProperty("place");
				double distance1 = pointGeoshape.getPoint().distance(pointGeoshape2.getPoint());
				Graph.edgesMap.put(""+id_vertex1+"-"+id_vertex2, distance1);
			}
			
		}
	}

}
