import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


public class Mapper{
	
	public HashMap<String, String> enCoding;
	public HashMap<String, String> deCoding;
	
	public Mapper() throws SQLException{
		enCoding = new HashMap<String,String>();
		deCoding = new HashMap<String,String>();
		init();
	}
	
	public void init() throws SQLException
	{
		Statement stmt;
		
		MySql mySql = new MySql();		
		Connection connection = (Connection) mySql.connect();
		
		if (connection == null) {
			System.out.println("Connection Error!!");
		}else{
			stmt = (Statement) connection.createStatement();
	        String sql = "SELECT DISTINCT primary_type FROM dataset ORDER BY primary_type";
	        ResultSet rs = stmt.executeQuery(sql);
	        int count = 1;
	        while(rs.next()){
	        	String type = rs.getString("primary_type");
	        	enCoding.put(type, ""+count);
	        	deCoding.put(""+count, type);
	        	count++;
	        }
		}
	}
	
	public HashMap<String, String> getEncoding(){
		return enCoding;
	}
	
	public HashMap<String, String> getDecoding(){
		return deCoding;
	}
}