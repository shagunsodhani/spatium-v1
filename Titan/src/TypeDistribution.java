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

import org.apache.cassandra.cli.CliParser.countStatement_return;
import org.ietf.jgss.Oid;

	import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.sleepycat.je.latch.Latch;
import com.sun.org.apache.xpath.internal.operations.And;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Geoshape.Point;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

public class TypeDistribution {
		
//		public static Map<Geoshape, Integer> typeDist;
		
		public static double[][][] typeDist;
		public static int[][] count;
			
		public static void writeDB() throws SQLException{
			
			Statement stmt;
			Connection connection  = (Connection) new MySql().getConnection();
			
//			MySql mySql = new MySql();
//			Connection conn = (Connection) mySql.connect();
			if (connection == null) {
				System.out.println("Connection Error!!");
			}
			else{
				System.out.println("Creating statement...");
			      
				  stmt = (Statement) connection.createStatement();
				  String sql_truncate = "TRUNCATE results";
				  stmt.executeUpdate(sql_truncate);
				  
				  String sql = "INSERT INTO results (latitude, longitude, count) VALUES ";
				  int temp;
				  for(int i=0;i<100;i++){
					  for(int j=0;j<100;j++){
						  temp = count[i][j];
						  if(temp!=0){
							  sql = sql+"("+typeDist[i][j][0]+","+typeDist[i][j][1]+","+temp+"), ";
						  }
					  }
				  }
				  sql = sql.substring(0, sql.length()-2);
//				  System.out.println(sql);
				  
				  int rs = stmt.executeUpdate(sql);
				  
			    try{
			         if(stmt!=null)
			            connection.close();
			      }catch(SQLException se){
			      }// do nothing
			      try{
			         if(connection!=null)
			            connection.close();
			      }catch(SQLException se){
			         se.printStackTrace();
			      }
			}
		}
		
		public static void main(String[] args) throws IOException, InterruptedException{
			
//			typeDist = new ConcurrentHashMap<Geoshape, Integer>();
			
			typeDist = new double[100][100][2];
			
//			BufferedReader br = new BufferedReader( new InputStreamReader(System.in));
//			String type = br.readLine();
			String type = args[0];
			for (int k = 1; k < args.length; k++) {
				type = type+" "+args[k];
			}
			
//			String type = args[0];
			System.out.println("Type is "+type);
			
			Database db = Database.getInstance();
			TitanGraph graph = db.getTitanGraph();
			ExecutorService executorService = Executors.newFixedThreadPool(100);
					
			double startLat = 41.644580105,startLong = -87.934324986,endLat = 42.023024908,endLong = -87.524388789;
			int width = 100, height = 100;

			double delta_lat = (endLat-startLat)/height;
			double delta_long = (endLong-startLong)/width;
			count =  new int[100][100];

			for(int i=0;i<100;i++){
				for(int j=0;j<100;j++){
					count[i][j] = 0;
				}
			}
			
			for(Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL,type).vertices().iterator();
					iterator.hasNext();){
			
				Vertex vertex = iterator.next();
				Geoshape point = vertex.getProperty("place");
				double lat = point.getPoint().getLatitude();
				double lon = point.getPoint().getLongitude();
				if(lat!=0.0 & lon!=0.0){
					int i = (int) ((lat-startLat)/delta_lat);
					int j = (int)((lon-startLong)/delta_long);
					typeDist[i][j][0] = lat;
					typeDist[i][j][1] = lon;
//					System.out.println(i+" "+j+" "+lat+" "+lon);
					count[i][j] +=1;
				}
				
			}
			
//			for(int i=0;i<100;i++){
//				for(int j=0;j<100;j++){
//					System.out.println(count[i][j]+" "+typeDist[i][j][0]+" "+typeDist[i][j][1]);
//				}
//				System.out.println();
//			}
			
			try {
				writeDB();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			db.closeTitanGraph(graph);
		}

}
