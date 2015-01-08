import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Database {
	
	public Database()
	{
		System.out.println("Default Constructor for Database class called.");
	}
	
	public TitanGraph connect(){
		
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
		config.addProperty("storage.keyspace", prop.getProperty("storage.keyspace"));
		
		TitanGraph g = TitanFactory.open(config);
		System.out.println("Instantiated Titan Graph Instance");
		return g;
	}
	
	public void close(TitanGraph graph){
		/**
		 * Method to close graph database connection
		 */
		graph.shutdown();
		System.out.println("Connection Closed.");
	}
		
	public static void main(String[] args){

		
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
