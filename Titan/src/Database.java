import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
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
		System.out.println("Default Constructor for Database class called.\n");
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
		/*
		 * Adding properties related to backend storage to config
		 * for eg: cassandra
		 */
		config.addProperty("storage.backend",prop.getProperty("storage.backend"));
		config.addProperty("storage.hostname", prop.getProperty("storage.hostname"));
		config.addProperty("storage.keyspace", prop.getProperty("storage.keyspace"));
//		config.addProperty("storage.batch-loading",prop.getProperty("storage.batch-loading"));
		config.addProperty("ids.block-size",prop.getProperty("ids.block-size"));
//		System.out.proprintln(config.getProperties("storage.buffer-size"));
//		System.out.println(config.getProperties(""));
		//		storage.buffer-size
		
		/*
		 * Adding properties related to external indexing
		 * for eg: elasticsearch  
		 */
		config.addProperty("index.search.backend", prop.getProperty("storage.index.search.backend"));
		config.addProperty("index.search.hostname", prop.getProperty("storage.index.search.hostname"));
		config.addProperty("index.search.client-only",prop.getProperty("storage.index.search.client-only"));

		
		TitanGraph g = TitanFactory.open(config);
		System.out.println("Instantiated Titan Graph Instance");
		return g;
	}
	
	public void close(TitanGraph graph){
		/**
		 * Method to close graph database connection
		 */
		graph.commit();
		graph.shutdown();
		System.out.println("Connection Closed.\n");
	}	
}
