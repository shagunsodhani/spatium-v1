import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
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
		 * This closes the connection to graph database as well.
		 */
		db.close(graph);
		TitanCleanup.clear(graph);
		System.out.println("Cleared the graph.\n");
		
		TitanGraph clear_graph = db.connect();
		return clear_graph;
	}
	
	public static void iterateVertices(TitanGraph graph){
		
		System.out.println("IterateVertices function called.\n");
		int counter = 0;
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			System.out.println("Id = "+vertex.getId()+" Latitude = "+vertex.getProperty("latitude")+" Longitude = "+
								vertex.getProperty("longitude")+" Type = "+vertex.getProperty("type"));
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
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		/*
		System.out.println("Starting insertion of vertices");
		for(int i = 0; i < 1000000; i++){
			graph.addVertex(i);
		}
		System.out.println("Added all the vertices");
		
		date = new Date();
		System.out.println(dateFormat.format(date));
		
		
				
		System.out.println("Deleted all the vertices");
		date = new Date();
		System.out.println(dateFormat.format(date));
		db.close(clearGraph);
		*/
		
		
		InitializeGraph(graph);
//		System.out.println("Graph initialized\n");
		date = new Date();
		System.out.println(dateFormat.format(date));
//		TitanGraph clearGraph = clearGraph(db,graph);
//		iterateVertices(clearGraph);
//		db.close(clearGraph);
//		
//		iterateVertices(graph);
		db.close(graph);
		
	}

}
