import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.commons.collections.functors.ForClosure;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query.Compare;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author precise
 *
 */
@SuppressWarnings("deprecation")
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
		double time_1 = System.currentTimeMillis();
		for (Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			vertex.setProperty("visible", false);
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			/*
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+
								vertex.getProperty("type")+" Latitude = "+latitude+ " Longitude = "+longitude+"\n");
			
			System.out.println("Vertices in "+distance+" km of locality are : ");
			*/
			// counter1 variable stores total no. of vertices satisfying Geo.WITHIN or its no. of edges
			int counter1 = 0;
			
			for (Iterator <Vertex> iterator2 = graph.query().has("type",Compare.NOT_EQUAL, type).has("visible",Compare.EQUAL,true).has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).vertices().iterator();
					iterator2.hasNext();) {
				Vertex vertex2 = iterator2.next();
//				System.out.println(counter+" : "+"Id = "+vertex2.getId()+" Place = "+vertex2.getProperty("place")+" Type = "+vertex2.getProperty("type"));
				
				//Get other point
				Geoshape pointGeoshape2 = vertex2.getProperty("place");
				String labelString = vertex2.getProperty("type")+"-"+type;
				
//				Add edge between instances of two different types with label as type1-type2 eg:BATTERY-NARCOTICS only if NARCOTICS-BATTERY edge is not present
//			    vertex.query().has("id", vertex2.getId()).direction(Direction.BOTH).has(labelString, vertex2).vertices();
				Edge edge = vertex.addEdge(type+"-"+vertex2.getProperty("type"), vertex2);
					
//				Set property for edge as distance between two vertices
				edge.setProperty("distance",pointGeoshape.getPoint().distance(pointGeoshape2.getPoint()));
				counter1++;
			}
//			System.out.println("Vertices in nearby locality are : "+counter1+"\n");
//			System.out.println("No. of edges added are = "+counter1);			
			counter += counter1;			
		}
		double time_2 = System.currentTimeMillis();
		System.out.println("Total no. of edges added for type = "+type+" are = "+counter+" in "+(time_2-time_1)+ "ms.\n");
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
				edge.getVertex(Direction.IN).setProperty("visible", true);
				edge.getVertex(Direction.OUT).setProperty("visible", true);
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
		
	public static TitanGraph exp1(Database db, TitanGraph graph) throws Exception{
		/*
		 * Need to return Titangraph instance because clearGraph functions clears the graph and returns new instance
		 * on which further processing needs to be done.
		 */
		
		graph = Graph.clearGraph(db, graph);
		Graph.InitializeGraph(graph);
		Map<String, Integer> typeMap = Socrata.statistics(graph);
//		Graph.removeEdges(graph);
		
		Graph.addEdges(graph, "THEFT",0.2);
		Graph.addEdges(graph, "NARCOTICS", 0.2);
		
//		Graph.exploreNeighboursGeo(graph, "THEFT", 0.2);
//		Graph.exploreNeighboursEdge(graph, "THEFT", 0.2);
//		Graph.removeEdges(graph);
//		Graph.clearGraph(db, graph);
//		db.close(graph);
		return graph;
	}
	
	public static void exploreNeighboursGeo(TitanGraph graph, String type, double distance) {
		
		System.out.println("Exploring neighbours using Geoshape and Geo.WITHIN i.e, using elasticsearch");
		int counter = 0;
//		double time = 0;
		double time_1 = System.currentTimeMillis();
		
		for (Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
//			double time_3 = System.currentTimeMillis();
			Vertex vertex = iterator.next();
			
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			
			Iterator<Vertex>iterator2  = graph.query().has("type",Compare.NOT_EQUAL, type).has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).vertices().iterator();
			
//			double time_4 = System.currentTimeMillis();
			
//			System.out.println("time required for "+vertex.getId()+" = "+(time_4-time_3));
//			time += time_4-time_3;
			counter++;
		}
		double time_2 = System.currentTimeMillis();
		
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
//		System.out.print("Time excluding I/O "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
		
	}
	
	public static void exploreNeighboursEdge(TitanGraph graph, String type, double distance) {
		
		System.out.println("Exploring neighbours using Edge Traversal");
		double time = 0;
		double time_1 = System.currentTimeMillis();
		int counter = 0;
		
		for (Iterator<Vertex> iterator = graph.query().has("type", Compare.EQUAL, type).vertices().iterator();iterator.hasNext();) {
//			double time_3 = System.currentTimeMillis();
		    Vertex vertex = iterator.next();
		    Iterator<Vertex> iterator2 = vertex.query().vertices().iterator();
		    /*
		     * First Method - Returns a list of vertices connected to a vertex under consideration irrespective of its distance
		     * from that vertex. 
		     */

		    /*
		    for(Iterator<Vertex> iterator2 = vertex.query().vertices().iterator();iterator2.hasNext();){
		    	Vertex vertex2 = iterator2.next();
		    	System.out.println("Vertex : "+vertex.getId()+" Vertex_In : "+vertex2.getId());
		    }
		    */
		    
		    /*
		     * Second Method - Returns a list of edges(both - OUT and IN) which can be used to filter neighbouring vertices
		     * based on distance threshold. Filtering need to be implemented. 
		     */
//		     
		    /*
		    for (Iterator<Edge> iterator2 = vertex.query().edges().iterator();iterator2.hasNext();) {
				Edge edge = iterator2.next();
				Vertex vertex2 = edge.getVertex(Direction.IN);
				System.out.println("Vertex : "+vertex.getId()+" Vertex_In : "+vertex2.getId());
			}
			*/
//		    double time_4 = System.currentTimeMillis();
//		    System.out.println("time required for "+vertex.getId()+" = "+(time_4-time_3));
//			time += time_4-time_3;
			counter++;
		    
		}
		double time_2 = System.currentTimeMillis();
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
//		System.out.print("Time excluding I/O "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
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
		System.out.println(dateFormat.format(date));
		
		graph = exp1(db, graph);
		
//		graph = clearGraph(db,graph);
//		iterateVertices(graph);
			
//		InitializeGraph(graph);
//		System.out.println("Graph initialized\n");
		
//		Map<String, Integer> typeMap = Socrata.statistics(graph);
//		addEdges(graph,0.2);
//		iterateVertices(graph);
		
//		addEdges(graph, "STALKING",0.2);
//		iterateEdges(graph);
//		removeEdges(graph);
				
		date = new Date();
		System.out.println(dateFormat.format(date));
		db.close(graph);
		
	}

}
