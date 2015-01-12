import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class MySql {

	    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static String DB_URL = "jdbc:mysql://";
	    static String USER,PASS,HOSTNAME,DATABASE,TABLE;		
		
		public static Connection connect() {			   
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
			USER = prop.getProperty("mysql.user");
			PASS = prop.getProperty("mysql.passwd");
			HOSTNAME = prop.getProperty("mysql.hostname");
			
			//DB_URL = jdbc:mysql://hostname/database_name
			DB_URL = DB_URL+HOSTNAME+"/"+DATABASE;
			   
			Connection conn = null;
			try{
			      Class.forName("com.mysql.jdbc.Driver");
//			      System.out.println("Connecting to a selected database..."+DB_URL);
			      conn = DriverManager.getConnection(DB_URL, USER, PASS);
//			      System.out.println("Connected database successfully...");
			      return conn;
			   }catch(SQLException se){
			      //Handle errors for JDBC
			      se.printStackTrace();
			   }catch(Exception e){
			      //Handle errors for Class.forName
			      e.printStackTrace();
			   }
			return conn;
			}
		}