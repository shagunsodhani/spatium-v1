import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

public class MongoDB {
	
	private static MongoClient mMongoClient;
	private String databaseName;
	
	public MongoDB(){
		Properties prop = new Properties();
		InputStream input = null;
		try {	 
			input = new FileInputStream("config/config.properties");
			prop.load(input);
			input.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		databaseName = prop.getProperty("mongoDB.database");
		if (mMongoClient == null){
			String hostname, port, mongoConnectionsPerHost;
			hostname = prop.getProperty("mongoDB.hostname");
			port = prop.getProperty("mongoDB.port");
			mongoConnectionsPerHost = prop.getProperty("mongoDB.connectionsPerHost");

			MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(Integer.parseInt(mongoConnectionsPerHost)).build();		
			mMongoClient = new MongoClient( new ServerAddress(hostname, Integer.parseInt(port)) , options);
		}
	}
	
	
	public MongoDB(String database){
		
		Properties prop = new Properties();
		InputStream input = null;
		try {	 
			input = new FileInputStream("config/config.properties");
			prop.load(input);
			input.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		databaseName = database;
		if (mMongoClient == null){
			String hostname, port, mongoConnectionsPerHost;
			hostname = prop.getProperty("mongoDB.hostname");
			port = prop.getProperty("mongoDB.port");
			mongoConnectionsPerHost = prop.getProperty("mongoDB.connectionsPerHost");
			
			MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(Integer.parseInt(mongoConnectionsPerHost)).build();		
			mMongoClient = new MongoClient( new ServerAddress(hostname, Integer.parseInt(port)) , options);
		}
	}
	
	public MongoDatabase getMongoDatabase(boolean verbose){
		if(verbose==true){
			System.out.println("Connection opened with MongoDB database named "+databaseName);
		}		
		return mMongoClient.getDatabase(databaseName);
	}
	
	public MongoClient getMongoClient(){
		return mMongoClient;
	}
}
