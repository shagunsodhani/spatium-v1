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
		static String DB_URL = "jdbc:mysql://";//192.168.111.180/spatium";
		
		
		//  Database credentials
	    static String USER,PASS,HOSTNAME,DATABASE,TABLE;		
		
		private final int START = 0;
		private final int MAX_LIMIT = 10;//50000;
		private final int MAX_OFFSET = 50000;//5707643;
		
		public MySql(TitanGraph graph) {
			   
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
			DATABASE = prop.getProperty("mysql.database");
			TABLE = prop.getProperty("mysql.table");
			
			//DB_URL = jdbc:mysql://hostname/database_name
			DB_URL = DB_URL+HOSTNAME+"/"+DATABASE;
			   
			Connection conn = null;
			Statement stmt = null;
			
			try{
			      //STEP 2: Register JDBC driver
			      Class.forName("com.mysql.jdbc.Driver");

			      //STEP 3: Open a connection
			      System.out.println("Connecting to a selected database..."+DB_URL);
			      conn = DriverManager.getConnection(DB_URL, USER, PASS);
			      System.out.println("Connected database successfully...");
			      
			      System.out.println("Creating statement...");
			      stmt = (Statement) conn.createStatement();

			      String sql = "SELECT * FROM "+TABLE+" LIMIT "+START+","+MAX_LIMIT;
			      ResultSet rs = stmt.executeQuery(sql);
			      
			      // Building Schema for graph database
//			      MySql.build_schema(graph);
			      
			      long id;
		 		  double latitude, longitude, time = 0;
		 		  String type = null;
		 		  long time_1 = System.currentTimeMillis();
		 		  int count = START;
		 		 
			      //STEP 5: Extract data from result set
			      while(rs.next()){
			         //Retrieve by column name
			         id  = rs.getInt("id");
			         latitude = rs.getDouble("latitude");
			         longitude = rs.getDouble("longitude");
			         type = rs.getString("primary_type");
			         
			         System.out.println(id+" "+latitude+" "+longitude+" "+type);
			         /*
			         Geoshape place = Geoshape.point(latitude, longitude);
		 		        
		 		     Vertex node = graph.addVertex(id);
		 		     node.setProperty("type", type);
		 		     node.setProperty("place", place);
		 		     node.setProperty("visible", 1);
		 		     count++;
		 		     */
			      }
			    long time_2 = System.currentTimeMillis();
		 		time += time_2-time_1; 
		 		System.out.println("Total vertices added till now = "+count+" in "+(time_2-time_1)+" ms.");
			      rs.close();
			   }catch(SQLException se){
			      //Handle errors for JDBC
			      se.printStackTrace();
			   }catch(Exception e){
			      //Handle errors for Class.forName
			      e.printStackTrace();
			   }finally{
			      //finally block used to close resources
			      try{
			         if(stmt!=null)
			            conn.close();
			      }catch(SQLException se){
			      }// do nothing
			      try{
			         if(conn!=null)
			            conn.close();
			      }catch(SQLException se){
			         se.printStackTrace();
			      }//end finally try
			   }//end try
			   System.out.println("Goodbye!");
			}

		public static void build_schema(TitanGraph graph){
			
			long time_1 = System.currentTimeMillis();
			TitanManagement mgmt = graph.getManagementSystem();
			
			PropertyKey typeKey = mgmt.makePropertyKey("type").dataType(String.class).make();
			PropertyKey placeKey = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
//			PropertyKey timeKey = mgmt.makePropertyKey("time").dataType(Timestamp.class).make();
			PropertyKey distanceKey = mgmt.makePropertyKey("distance").dataType(Double.class).make();
			PropertyKey visibleKey = mgmt.makePropertyKey("visible").dataType(Integer.class).make();
							
			mgmt.buildIndex("type", Vertex.class).addKey(typeKey).buildCompositeIndex();
			mgmt.buildIndex("place", Vertex.class).addKey(placeKey).buildCompositeIndex();
			mgmt.buildIndex("place", Vertex.class).addKey(placeKey).buildMixedIndex("search");
//			mgmt.buildIndex("time",Vertex.class).addKey(timeKey).buildMixedIndex("search");
			mgmt.buildIndex("distance", Edge.class).addKey(distanceKey).buildCompositeIndex();
			mgmt.buildIndex("visible", Vertex.class).addKey(visibleKey).buildCompositeIndex();
			
			mgmt.commit();
			long time_2 = System.currentTimeMillis();
			
			System.out.println("Schema built in "+(time_2-time_1)+" ms.");
		}

}




 
 