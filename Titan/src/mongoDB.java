import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

/**
 * 
 */

/**
 * @author precise
 *
 */
public class mongoDB {
	
	static String HOSTNAME,PORT;
	String DATABASE;
	public static MongoClient mongoClient;
	
	public mongoDB(){
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
		
		HOSTNAME = prop.getProperty("mongoDB.hostname");
		DATABASE = prop.getProperty("mongoDB.database");
		PORT = prop.getProperty("mongoDB.port");
		MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(1000).build();		
		mongoClient = new MongoClient( new ServerAddress(HOSTNAME, Integer.parseInt(PORT)) , options);
		System.out.println(mongoClient.getMongoClientOptions().getConnectionsPerHost());
	}
	
	public mongoDB(String database){
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
		
		HOSTNAME = prop.getProperty("mongoDB.hostname");
		DATABASE = database;
		PORT = prop.getProperty("mongoDB.port");
		MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(200).build();		
		mongoClient = new MongoClient( new ServerAddress(HOSTNAME, Integer.parseInt(PORT)) , options);
		System.out.println(mongoClient.getMongoClientOptions().getConnectionsPerHost());
	}
	
	public MongoDatabase connect(boolean verbose){
				
		MongoDatabase db = mongoClient.getDatabase(DATABASE);
		if(verbose==true){
			System.out.println("Connection opened with MongoDB database named "+DATABASE);
		}		
		return db;
	}
	
	public void disconnect(MongoDatabase db){
	
	}
}
