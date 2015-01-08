import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
		 * Delete all the vertices from graph database
		 */
		int count = 1;
		for (Vertex vertex : g.getVertices()){
			g.removeVertex(vertex);
			count+=1;
			if(count%1000==0){
				System.out.println(count);
			}
		}
		System.out.println("Deleted all vertices");
	}
	
	public static void main(String[] args){
		TitanGraph g = connect();
		clear_graph(g);
		Vertex juno = g.addVertex(null);
		juno.setProperty("name", "juno");
		Vertex jupiter = g.addVertex(null);
		jupiter.setProperty("name", "jupiter1");
		Edge married = g.addEdge(null, juno, jupiter, "married");
		for(Vertex vertex : g.getVertices()) {
			  System.out.println(vertex.getProperty("id")); 
			}
	}
}
