import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class Database {
	
	public static TitanGraph connect(){
		
		/**
		 * Method to connect to the graph database
		 */
		
		Properties prop = new Properties();
		InputStream input = null;
	 
		try {	 
			input = new FileInputStream("config/config.properties");
			prop.load(input);
	 
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		Configuration config = new BaseConfiguration();
		config.addProperty("storage.backend",prop.getProperty("storage.backend"));
		config.addProperty("storage.hostname", prop.getProperty("storage.hostname"));
		
		TitanGraph g = TitanFactory.open(config);
		System.out.println("Opened TitanFactory");
		return g;
	}
	
	public static void clear_graph(TitanGraph g){
		/**
		 * Delete all the vertices and edges from graph database.
		 * This closes the connection to graph database as well.
		 */
		g.shutdown();
		TitanCleanup.clear(g);
		System.out.println("Deleted the graph");
	}
	
	public static void main(String[] args){

		TitanGraph g = connect();
		clear_graph(g);
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		System.out.println("Starting insertion of vertices");
		for(int i = 0; i < 1000000; i++){
			g.addVertex(i);
		}
		System.out.println("Added all the vertices");
		
		date = new Date();
		System.out.println(dateFormat.format(date));
		
		clear_graph(g);
		
		System.out.println("Deleted all the vertices");
		date = new Date();
		System.out.println(dateFormat.format(date));

//		Vertex juno = g.addVertex(null);
//		juno.setProperty("name", "juno");
//		Vertex jupiter = g.addVertex(null);
//		jupiter.setProperty("name", "jupiter1");
//		Edge married = g.addEdge(null, juno, jupiter, "married");
//		for(Vertex vertex : g.getVertices()) {
//			  System.out.println(vertex.getProperty("id")); 
//			}
		
	}
}
