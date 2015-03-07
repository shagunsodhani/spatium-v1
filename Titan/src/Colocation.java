import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
//import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Vertex;

public class Colocation {
	
	public static Database db = new Database();
	public static TitanGraph graph = db.connect();
	public static HashMap<String, Integer> total_count = new HashMap<String, Integer>(); 
	public double PI_threshold = 1;
	
	public static void L1(){
		/*
		 * Generate colocations of size 1
		 * Iterate over all vertices of the graph
		 */

		System.out.println("Generating colocations of size 1.\n");
		int counter = 0;
	    //could lead to buffer-overflow

		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator.hasNext();) {
				Vertex vertex = iterator.next();
				String type = vertex.getProperty("type");
				if (total_count.containsKey(type))
				{
					total_count.put(type, total_count.get(type)+1);
				}
				else {
					total_count.put(type, 1);
				}
				System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type")+" Visible = "+vertex.getProperty("visible"));
				counter++;
			}
			System.out.println("Total number of colocations of size 1 = "+counter+"\n");
		}
	
	public static void L2 (){
		/*
		 * Generate colocations of size 2
		 * Iterate over all edges of the graph
		 */
		
		System.out.println("Generating colocations of size 2.\n");
		int counter = 0;
		//could lead to buffer-overflow
		
		for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
			Edge edge = iterator.next();
			System.out.println(counter+" : "+" Edge Label = "+edge.getLabel()+" Distance = "+edge.getProperty("distance"));
			counter++;
		}
		System.out.println("Total no. of colocations of size 2  are = "+counter);
		
	}
	
	public static void Lk(){
		/*
		 * Generate colocation of size k
		 */
	}
	public static void main(String[] args) {
		
		L2();

	}

}
