import java.util.Iterator;

import org.elasticsearch.bootstrap.Elasticsearch;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Query.Compare;

public class EdgeInsertion extends Thread{
	public TitanTransaction graph;
	public Vertex vertex;
	public double original_distance, new_distance;
	
	public EdgeInsertion(TitanTransaction graph, long id, double distance) {
		this.graph = graph;
		this.vertex = graph.getVertex(id);
		this.original_distance = distance;
		this.new_distance = 0.0;
	}
	
	public EdgeInsertion(TitanTransaction graph, long id, double distance1, double distance2) {
		this.graph = graph;
		this.vertex = graph.getVertex(id);
		this.original_distance = distance1;
		this.new_distance = distance2;
	}
	
	public void run(){
		
		vertex.setProperty("visible", 0);
		Geoshape pointGeoshape = vertex.getProperty("place");
		double latitude = pointGeoshape.getPoint().getLatitude();
		double longitude = pointGeoshape.getPoint().getLongitude();
//		long id_vertex1 = Long.parseLong(vertex.getId().toString());
		String type = vertex.getProperty("type");
		Elasticsearch elasticsearch = new Elasticsearch();
		
		Iterator<Vertex>iterator = null;
		if(original_distance == new_distance){
			iterator = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, original_distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator();
		}else{
			iterator = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, new_distance)).has("place", Geo.DISJOINT, Geoshape.circle(latitude, longitude, original_distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator();
		}
		
		for(;iterator.hasNext();){
			Vertex vertex2 = iterator.next();
//			long id_vertex2 = Long.parseLong(vertex2.getId().toString());
			String type2 = vertex2.getProperty("type");
			
			if(Integer.parseInt(type)<Integer.parseInt(type2)){
				Geoshape pointGeoshape2 = vertex2.getProperty("place");
				float distance = (float)pointGeoshape.getPoint().distance(pointGeoshape2.getPoint());
//				graph.addEdge(vertex, vertex2, type+"-"+type2)
				Edge edge = vertex2.addEdge(type+"-"+type2,vertex);
				edge.setProperty("distance",distance);				
//				Graph.edgesMap.put(""+id_vertex1+"-"+id_vertex2, distance1);
			}			
		}
//		graph.commit();
	}

}
