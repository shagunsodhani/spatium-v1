import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MySql {
	
	private Connection mConnection;
	
	public MySql() {
		
		String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		String DB_URL = "jdbc:mysql://";
	    String USER, PASS, HOSTNAME, DATABASE;
	    Properties prop = new Properties();
		InputStream input = null;
		try {	 
			input = new FileInputStream("config/config.properties");
			prop.load(input);
			input.close();
	 
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		USER = prop.getProperty("mysql.user");
		PASS = prop.getProperty("mysql.passwd");
		HOSTNAME = prop.getProperty("mysql.hostname");
		DATABASE = prop.getProperty("mysql.database");
		DB_URL = DB_URL+HOSTNAME+"/"+DATABASE;
		
		try{
			Class.forName(JDBC_DRIVER);
		    System.out.println("Connecting to Mysql database : "+DB_URL);
		    mConnection = DriverManager.getConnection(DB_URL, USER, PASS);
		    System.out.println("Connected to database succesfully");
		}
		catch(SQLException | ClassNotFoundException e){
			//Handle errors for JDBC and ClassnotFoundException
		    e.printStackTrace();
		}
	}
    
	public Connection getConnection() {
		return mConnection;
	}
}