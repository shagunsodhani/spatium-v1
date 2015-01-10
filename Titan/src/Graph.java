import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.el.EqualsOperator;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;

import com.spatial4j.core.shape.impl.GeoCircle;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query.Compare;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * 
 */

/**
 * @author sanket
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
	
	@SuppressWarnings("unchecked")
	public static void addEdges(TitanGraph graph, String property, String type, double distance) {
		/*
		 * Method to compute vertices in the nearby region as specified by 'distance' argument with respect to vertices of type 
		 * specified by 'type' argument and add edges between them. 
		 */
		
		int counter = 0;

		for (Iterator<Vertex> iterator = graph.query().has(property,Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+
								vertex.getProperty("type")+" Latitude = "+latitude+ " Longitude = "+longitude+"\n");
			
			System.out.println("Vertices in "+distance+" km of locality are : ");
			int counter1 = 0;
			
			for (Iterator <Vertex> iterator2 = graph.query().has(property,Compare.NOT_EQUAL, type).has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).vertices().iterator();
					iterator2.hasNext();) {
				Vertex vertex2 = iterator2.next();
				System.out.println(counter+" : "+"Id = "+vertex2.getId()+" Place = "+vertex2.getProperty("place")+" Type = "+vertex2.getProperty("type"));
				counter1++;
			}
			System.out.println("Vertices in nearby locality are : "+counter1+"\n");
			
			counter++;			
		}
		System.out.println("Total Vertices = "+counter+"\n");
	}
		
	public static void iterateVertices(TitanGraph graph){
		/*
		 * Iterates over all vertices of the graph and displays total no. of vertices.
		 */
		
		System.out.println("IterateVertices function called.\n");
		int counter = 0;
        	
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type"));
			counter++;
		}
		System.out.println("Total Vertices = "+counter+"\n");
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Database db = new Database();
		TitanGraph graph = db.connect();
		
//		graph = clearGraph(db,graph);
//		iterateVertices(graph);
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		
		/*
		TitanManagement mgmt = graph.getManagementSystem();
        PropertyKey nameKey = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("name",Vertex.class).addKey(nameKey).buildMixedIndex("search");
        mgmt.commit();
		      
		System.out.println("Starting insertion of vertices");
		
		for(int i = 0; i < 1000; i++){
			Vertex vertex = graph.addVertex();
			vertex.setProperty("name", ""+i);
		}
		System.out.println("Added all the vertices");
		
		date = new Date();
		System.out.println(dateFormat.format(date));
		*/
		
//		InitializeGraph(graph);
//		System.out.println("Graph initialized\n");
//		iterateVertices(graph);
		
		addEdges(graph, "type","MOTOR VEHICLE THEFT",0.2);
		
		date = new Date();
		System.out.println(dateFormat.format(date));
		db.close(graph);
		
	}

}
