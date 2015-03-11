import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * 
 */

/**
 * @author precise
 *
 */
public class mongoDB {
	
	static String HOSTNAME,DATABASE,PORT;
	
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
	}
	
	public MongoDatabase connect(){
		
		MongoClient mongoClient = new MongoClient( HOSTNAME , Integer.parseInt(PORT) );
		MongoDatabase db = mongoClient.getDatabase(DATABASE);
		System.out.println("Connection opened with MongoDB database named "+DATABASE);
		return db;
	}
	
	public void disconnect(MongoDatabase db){
	
	}
}
