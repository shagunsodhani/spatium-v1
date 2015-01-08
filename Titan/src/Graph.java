import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;

/**
 * 
 */

/**
 * @author precise
 *
 */
public class Graph {
	
	public static TitanGraph clearGraph(Database db, TitanGraph graph){
		/**
		 * Delete all the vertices and edges from graph database.
		 * This closes the connection to graph database as well.
		 */
		db.close(graph);
		TitanCleanup.clear(graph);
		System.out.println("Cleared the graph.");
		
		TitanGraph clear_graph = db.connect();
		return clear_graph;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Database db = new Database();
		
		TitanGraph graph = db.connect();
		
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		
		System.out.println("Starting insertion of vertices");
		for(int i = 0; i < 1000000; i++){
			graph.addVertex(i);
		}
		System.out.println("Added all the vertices");
		
		date = new Date();
		System.out.println(dateFormat.format(date));
		
		TitanGraph clearGraph = clearGraph(db,graph);
				
		System.out.println("Deleted all the vertices");
		date = new Date();
		System.out.println(dateFormat.format(date));

		db.close(clearGraph);
	}

}
