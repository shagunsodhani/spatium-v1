import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.el.EqualsOperator;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;

import com.spatial4j.core.shape.impl.GeoCircle;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query.Compare;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author precise
 *
 */
public class Graph {	
	
	@SuppressWarnings("unused")
	public static void InitializeGraph(TitanGraph graph) throws Exception
	{
		System.out.println("InitializeGraph method called.\n");
		Socrata http = new Socrata(graph);
		
	}
	
	public static TitanGraph clearGraph(Database db, TitanGraph graph){
		/**
		 * Delete all the vertices and edges from graph database.
		 * This closes the connection to graph database as well and reopens a database connection and return graph instance
		 */
	
		db.close(graph);
		TitanCleanup.clear(graph);
		System.out.println("Cleared the graph.\n");
		
		TitanGraph clear_graph = db.connect();
		return clear_graph;
	}
	
	public static void addEdges(TitanGraph graph, double distance){
		
		Map<String, Integer> typeMap = Socrata.statistics(graph);
		
	}
	
	@SuppressWarnings("unchecked")
	public static void addEdges(TitanGraph graph, String type, double distance) {
		/*
		 * Method to compute vertices in the nearby region as specified by 'distance' argument with respect to vertices of type 
		 * specified by 'type' argument and add edges between them. 
		 */
		
		int counter = 0;

		for (Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+
								vertex.getProperty("type")+" Latitude = "+latitude+ " Longitude = "+longitude+"\n");
			
			System.out.println("Vertices in "+distance+" km of locality are : ");
			
			// counter1 variable stores total no. of vertices satisfying Geo.WITHIN or its no. of edges
			int counter1 = 0;
			
			for (Iterator <Vertex> iterator2 = graph.query().has("type",Compare.NOT_EQUAL, type).has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).vertices().iterator();
					iterator2.hasNext();) {
				Vertex vertex2 = iterator2.next();
//				System.out.println(counter+" : "+"Id = "+vertex2.getId()+" Place = "+vertex2.getProperty("place")+" Type = "+vertex2.getProperty("type"));
				
				//Get other point
				Geoshape pointGeoshape2 = vertex2.getProperty("place");
				
				//Add edge between instances of two different types with label as type1-type2 eg:BATTERY-NARCOTICS
				Edge edge = vertex.addEdge(type+"-"+vertex2.getProperty("type"), vertex2);
				
				//Set property for edge as distance between two vertices
				edge.setProperty("distance",pointGeoshape.getPoint().distance(pointGeoshape2.getPoint()));
				
				counter1++;
			}
//			System.out.println("Vertices in nearby locality are : "+counter1+"\n");
//			System.out.println("No. of edges added are = "+counter1);			
			counter += counter1;			
		}
		System.out.println("Total no. of edges added are = "+counter+"\n");
	}
		
	public static void iterateVertices(TitanGraph graph){
		/*
		 * Iterates over all vertices of a graph and displays total no. of vertices.
		 */
		
		System.out.println("iterateVertices function called.\n");
		int counter = 0;
        	
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type"));
			counter++;
		}
		System.out.println("Total Vertices = "+counter+"\n");
	}
	
	public static void iterateEdges(TitanGraph graph) {
		/*
		 * Iterates over all edges of a graph and displays total no. of edges
		 */
		
		System.out.println("iterateEdges function called.\n");
		int counter = 0;
		
		for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
			Edge edge = iterator.next();
			System.out.println(counter+" : "+" Edge Label = "+edge.getLabel()+" Distance = "+edge.getProperty("distance"));
			counter++;
		}
		System.out.println("Total no. of edges are = "+counter);		
	}
	
	public static void removeEdges(TitanGraph graph){
		/*
		 * Removes all edges of the form type1-type2 and type2-type1
		 */
		System.out.println("removeEdges function called.\n");
		int counter = 0;
					
			for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
				Edge edge = iterator.next();
				edge.remove();
				counter++;
			}		
		System.out.println("Total no. of edges removed were = "+counter);		
	}
	
	public static void removeEdges(TitanGraph graph, String type1, String type2) {
		String labelString ="";
		
		if (type1 == null || type2 == null) {
			labelString.concat(type1).concat(type2);
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		Database db = new Database();
		TitanGraph graph = db.connect();
		
//		graph = clearGraph(db,graph);
//		iterateVertices(graph);
				
		System.out.println(dateFormat.format(date));
	
//		InitializeGraph(graph);
//		System.out.println("Graph initialized\n");
		
//		Map<String, Integer> typeMap = Socrata.statistics(graph);
//		addEdges(graph,0.2);
//		iterateVertices(graph);
		
		addEdges(graph, "STALKING",0.2);
		iterateEdges(graph);
		removeEdges(graph);
				
		date = new Date();
		System.out.println(dateFormat.format(date));
		db.close(graph);
		
	}

}
