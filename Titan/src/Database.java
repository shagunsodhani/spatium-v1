import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.hadoop.HadoopFactory;
import com.thinkaurelius.titan.hadoop.HadoopGraph;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class to handle misc database functions
 *
 */
public class Database {
	
	private static Database mInstance;
	
//	Can make it synchronized later
	public static Database getInstance(){
		if(mInstance == null)
			mInstance = new Database();
		return mInstance;
	}
	
	private Database()
	{
	}
	
	public HadoopGraph getHadoopGraph() throws Exception{
		
		/**
		 * Method to connect to the titan-hadoop graph database
		 */
		HadoopGraph graph = HadoopFactory.open("config/titan-graphson.properties");
//		HadoopGraph graph = HadoopFactory.open("config/config.properties");
		System.out.println("Connected to Hadoop Graph");
		return graph;
	}
	
	public void closeHadoopGraph(HadoopGraph graph){
		graph.shutdown();
		System.out.println("Hadoop Graph Closed");
	}
	
	public TitanGraph getTitanGraph(){
		
		/**
		 * Method to connect to the graph database
		 */
		Properties prop = new Properties();
		InputStream input = null;
		try {	 
			input = new FileInputStream("/home/precise/spatium/Titan/config/config.properties");
			prop.load(input);
			input.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
		
		Configuration config = new BaseConfiguration();
		/*
		 * Adding properties related to backend storage to config
		 * for eg: cassandra
		 */
		config.addProperty("storage.backend",prop.getProperty("storage.backend"));
		config.addProperty("storage.hostname", prop.getProperty("storage.hostname"));
		config.addProperty("storage.keyspace", prop.getProperty("storage.keyspace"));
		config.addProperty("storage.batch-loading",prop.getProperty("storage.batch-loading"));
		config.addProperty("ids.block-size",prop.getProperty("ids.block-size"));
		config.addProperty("storage.read-time",prop.getProperty("storage.read-time"));
		
		/*
		 * Adding properties related to external indexing
		 * for eg: elasticsearch  
		 */
		config.addProperty("index.search.backend", prop.getProperty("storage.index.search.backend"));
		config.addProperty("index.search.hostname", prop.getProperty("storage.index.search.hostname"));
		config.addProperty("index.search.client-only",prop.getProperty("storage.index.search.client-only"));
		config.addProperty("index.search.geohash", prop.getProperty("storage.index.search.geohash"));
		config.addProperty("index.geohash_precision", prop.getProperty("storage.index.geohash_precision"));
		config.addProperty("index.geohash_prefix", prop.getProperty("storage.index.geohash_prefix"));
		
		return TitanFactory.open(config);
	}
	
	public void closeTitanGraph(TitanGraph graph){
		/**
		 * Method to close graph database connection
		 */
		graph.commit();
		graph.shutdown();
		System.out.println("Connection Closed.");
	}	
}
