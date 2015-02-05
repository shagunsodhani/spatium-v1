import java.awt.print.Printable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.sleepycat.je.latch.Latch;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Geoshape.Point;
import com.tinkerpop.blueprints.Compare;
import com.vividsolutions.jts.triangulate.quadedge.Vertex;


public class Visualization {
	
	public static Map<Geoshape, Integer> typeDist; 
	
	public static void writeDB() throws SQLException{
		
		Statement stmt;
		
		Connection conn = (Connection) MySql.connect();
		if (conn == null) {
			System.out.println("Connection Error!!");
		}
		else{
			System.out.println("Creating statement...");
		      
			  stmt = (Statement) conn.createStatement();
			  String sql_truncate = "TRUNCATE results";
			  stmt.executeUpdate(sql_truncate);
			  
			  String sql = "INSERT INTO results (latitude, longitude, count) VALUES ";
			  Iterator it = typeDist.entrySet().iterator();
			  Geoshape point = null;
			  double latitude, longitude;
			  int count;
			  
			  while (it.hasNext())
			  {
			        Map.Entry pairs = (Map.Entry)it.next();
			        point = (Geoshape)pairs.getKey();
			        latitude = point.getPoint().getLatitude();
			        longitude = point.getPoint().getLongitude();
			        count = (Integer)pairs.getValue();
			        sql = sql+"("+latitude+","+longitude+","+count+"), ";
			  }
			  sql = sql.substring(0, sql.length()-2);
//			  System.out.println(sql);
			  
			  int rs = stmt.executeUpdate(sql);
			  
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
		      }
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		
		typeDist = new ConcurrentHashMap<Geoshape, Integer>();
		
		BufferedReader br = new BufferedReader( new InputStreamReader(System.in));
		String type = br.readLine();
//		String type = args[0];
		System.out.println("Type is "+type);
		
		Database db = new Database();
		TitanGraph graph = db.connect();
		ExecutorService executorService = Executors.newFixedThreadPool(10000);
				
		double startLat = 41.644580105,startLong = -87.934324986,endLat = 42.023024908,endLong = -87.524388789;
		int width = 100, height = 100;

		double delta_lat = (endLat-startLat)/height;
		double delta_long = (endLong-startLong)/width;
		
		for(int i=0; i < height;i++){
			for(int j=0; j < width;j++){
				double templat = startLat + i*delta_lat;
				double templong = startLong + j*delta_long;
				Geoshape southWest = Geoshape.point(templat, templong);
				Geoshape northEast = Geoshape.point(templat + delta_lat, templong + delta_long);
				
				WorkThread workThread = new WorkThread(graph, southWest, northEast,type);
				executorService.execute(workThread);
			}
		}
		
		executorService.shutdown();
//		executorService.awaitTermination(120, TimeUnit.SECONDS);
//		executorService.shutdownNow();
		while(!executorService.isTerminated()){
			;
		}
		
//		Iterator it = typeDist.entrySet().iterator();
//		Geoshape point = null;
//		  double latitude, longitude;
//		  int count;
//		  
//		  while (it.hasNext())
//		  {
//		        Map.Entry pairs = (Map.Entry)it.next();
//		        point = (Geoshape)pairs.getKey();
//		        latitude = point.getPoint().getLatitude();
//		        longitude = point.getPoint().getLongitude();
//		        count = (Integer)pairs.getValue();
//		        System.out.println(latitude+" "+longitude+" "+count);
//		  }
		   	
				
		System.out.println("All the threads terminated successfully");		
		
		try {
			writeDB();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		graph.commit();
		
		db.close(graph);
	}
}
