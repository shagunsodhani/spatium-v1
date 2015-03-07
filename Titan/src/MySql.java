import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MySql {

	    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static String DB_URL = "jdbc:mysql://";
	    static String USER,PASS,HOSTNAME,DATABASE,TABLE;		
		
		public static Connection connect() {			   
			Properties prop = new Properties();
			InputStream input = null;
			try {	 
//				input = new FileInputStream("/home/precise/spatium/Titan/config/config.properties");
				input = new FileInputStream("config/config.properties");
//				input = new FileInputStream("../config/config.properties");
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
			DATABASE = prop.getProperty("mysql.database");
			DB_URL = DB_URL+HOSTNAME+"/"+DATABASE;
			   
			Connection conn = null;
			try{
			      Class.forName("com.mysql.jdbc.Driver");
			      System.out.println("Connecting to a selected database..."+DB_URL);
			      conn = DriverManager.getConnection(DB_URL, USER, PASS);
			      System.out.println("Connected database successfully...");
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