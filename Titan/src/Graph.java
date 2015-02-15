import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;

import cern.colt.matrix.doublealgo.Statistic;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphTransaction;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Query.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

/**
 * @author precise
 *
 */
@SuppressWarnings("deprecation")
public class Graph {
	
	public static Map<String, Integer> countMap;
	
	public static Map<String,Integer> typeFrequency;
	
	public static Map<String, Double> edgesMap;
	
	public static TitanGraph clearGraph(Database db, TitanGraph graph){
		/**
		 * Delete all the vertices and edges from graph database.
		 * This closes the connection to graph database as well and reopens a database connection and return graph instance
		 */
	
		db.close(graph);
		TitanCleanup.clear(graph);
		System.out.println("Cleared the graph");
		
		TitanGraph clear_graph = db.connect();
		return clear_graph;
	}
	
	public static void build_schema(TitanGraph graph){
		
		long time_1 = System.currentTimeMillis();
		TitanManagement mgmt = graph.getManagementSystem();
		
		PropertyKey typeKey = mgmt.makePropertyKey("type").dataType(String.class).make();
		PropertyKey placeKey = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
//		PropertyKey approxplaceKey = mgmt.makePropertyKey("approxplace").dataType(Geoshape.class).make();
		
				
//		PropertyKey placeKeyTSI = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
//		PropertyKey timeKey = mgmt.makePropertyKey("time").dataType(Timestamp.class).make();
		
		PropertyKey distanceKey = mgmt.makePropertyKey("distance").dataType(Double.class).make();
		PropertyKey visibleKey = mgmt.makePropertyKey("visible").dataType(Integer.class).make();
		EdgeLabelMaker label = mgmt.makeEdgeLabel("knows");
		label.make();
		
		mgmt.buildIndex("type", Vertex.class).addKey(typeKey).buildCompositeIndex();
//		mgmt.buildIndex("type", Vertex.class).addKey(typeKey).buildMixedIndex("search");
//		mgmt.buildIndex("place", Vertex.class).addKey(placeKeyTSI).buildCompositeIndex();
//		mgmt.buildIndex("place", Vertex.class).addKey(placeKey).buildMixedIndex("search");
		mgmt.buildIndex("node",Vertex.class).addKey(typeKey).addKey(placeKey).buildMixedIndex("search");
		
//		mgmt.buildIndex("approxplace", Vertex.class).addKey(approxplaceKey).buildMixedIndex("search");
		
//		mgmt.buildIndex("time",Vertex.class).addKey(timeKey).buildMixedIndex("search");
//		mgmt.buildIndex("distance", Edge.class).addKey(distanceKey).buildMixedIndex("search");
//		mgmt.buildIndex("distance", Vertex.class).addKey(distanceKey).buildMixedIndex("search");
		mgmt.buildIndex("visible", Vertex.class).addKey(visibleKey).buildCompositeIndex();
		mgmt.commit();
		long time_2 = System.currentTimeMillis();
		
		System.out.println("Schema built in "+(time_2-time_1)+" ms.");
	}
	
	@SuppressWarnings("unused")
	public static void InitializeGraph(TransactionalGraph graph, int limit) throws Exception
	{
		System.out.println("InitializeGraph method called.\n");
		int START = 0;
		int MAX_LIMIT = limit;
		
		Statement stmt;
		
		Connection conn = (Connection) MySql.connect();
		if (conn == null) {
			System.out.println("Connection Error!!");
		}else{
//			System.out.println("Creating statement...");
		      
			stmt = (Statement) conn.createStatement();

		      String sql = "SELECT * FROM dataset ORDER BY date ASC LIMIT "+START+","+MAX_LIMIT;
//		      System.out.println(sql);
		      ResultSet rs = stmt.executeQuery(sql);
//		      System.out.println("query printed");
		      long id;
	 		  double latitude, longitude, time = 0;
	 		  String type = null;
	 		  long time_1 = System.currentTimeMillis();
	 		  int count = START;
	 		  long time_3 = System.currentTimeMillis();
	 		  long time_4;
		      while(rs.next()){

		    	 id  = rs.getInt("id");
		         latitude = rs.getDouble("latitude");
		         longitude = rs.getDouble("longitude");
		         type = rs.getString("primary_type");
		         
//		         System.out.println(id+" "+latitude+" "+longitude+" "+type);

		         Geoshape place = Geoshape.point(latitude, longitude);
	 		     Geoshape approxplace = Geoshape.point((double)Math.round(latitude*100)/100, (double)Math.round(longitude*100)/100);  
	 		     Vertex node = graph.addVertex(id);
	 		     node.setProperty("type", type);
	 		     node.setProperty("place", place);
//	 		     node.setProperty("approxplace", approxplace);
	 		     node.setProperty("visible", 1);
	 		     node.setProperty("distance", 2.345);
	 		     count++;
//	 		     node.getProperty("place");
	 		     if(count%100000 == 0)
	 		     {
	 		    	time_4 = System.currentTimeMillis();
	 		    	 System.out.println("Total vertices added till now = "+count+" in "+(time_4-time_3)+" ms.");
	 		    	 time_3 = time_4;
	 		    	 graph.commit();
	 		     }
		      }                                             
		    long time_2 = System.currentTimeMillis();
	 		time += time_2-time_1; 
	 		System.out.println("Total vertices added till now = "+count+" in "+(time_2-time_1)+" ms.");
		    rs.close();
		    
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
	
	public static void stats(TitanGraph graph){
		/*
		 * 1. Distribution of total instances across different crime types.
		 */
		
		Map<String, Integer> typeMap = new HashMap<String, Integer>();
		int counter_type = 0;
		
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			String value = vertex.getProperty("type");
			if(!typeMap.containsKey(value)){
				typeMap.put(value, 1);
				counter_type++;
			}else{
				int temp = typeMap.get(value);
				typeMap.put(value, ++temp);
			}			
		}
		
		System.out.println("Distribution of instances across "+counter_type+" different types : ");
		
		Iterator it = typeMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        System.out.println(pairs.getKey() + " = " + pairs.getValue());
	    }
	    Graph.typeFrequency = typeMap;
	    
	}
	
	public static void addEdges(TitanGraph graph, double distance){
		
		double time =0,time_1,time_2, time_3, time_4;
		int count_edges= 0 ;
		time_3 = System.currentTimeMillis();
		
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator.hasNext();) {
			
			Vertex vertex = iterator.next();
			vertex.setProperty("visible", 0);
			long id_vertex1 = Long.parseLong(vertex.getId().toString());
			
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			String type = vertex.getProperty("type");
			
			time_1 = System.currentTimeMillis();
			
			
			
			for(Iterator<Vertex>iterator2  = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator()
					;	iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				long id_vertex2 = Long.parseLong(vertex2.getId().toString());
				
				if(id_vertex1<id_vertex2){
					Geoshape pointGeoshape2 = vertex2.getProperty("place");
					double distance1 = pointGeoshape.getPoint().distance(pointGeoshape2.getPoint());
		//				String label = type+"-"+vertex2.getProperty("type");
		//				String label = id_vertex1+"-"+id_vertex2;
		//				Edge edge = graph.addEdge(null,vertex2,vertex,label);
		//				edge.setProperty("distance",distance1);\
//					Vertex vertex1 = graph.getVertex(vertex_ids[0]);
//			        Vertex vertex2 = graph.getVertex(vertex_ids[1]);
			        vertex.addEdge("knows", vertex2);
			        count_edges++;
//					Graph.edgesMap.put(""+id_vertex1+"-"+id_vertex2, distance1);
				}
			}
			
			
			
			
//			for (Iterator <Vertex> iterator2 = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).has("visible",Compare.EQUAL,1).vertices().iterator();
//					iterator2.hasNext();) {
//				Vertex vertex2 = iterator2.next();
////				System.out.println(counter+" : "+"Id = "+vertex2.getId()+" Place = "+vertex2.getProperty("place")+" Type = "+vertex2.getProperty("type"));
//				
//				//Get other point
//				Geoshape pointGeoshape2 = vertex2.getProperty("place");
////				String labelString = vertex2.getProperty("type")+"-"+type;
//				
////				Add edge between instances of two different types with label as type1-type2 eg:BATTERY-NARCOTICS only if NARCOTICS-BATTERY edge is not present
////			    vertex.query().has("id", vertex2.getId()).direction(Direction.BOTH).has(labelString, vertex2).vertices();
//				Edge edge = vertex.addEdge(type+"-"+vertex2.getProperty("type"), vertex2);
//					
////				Set property for edge as distance between two vertices
//				edge.setProperty("distance",pointGeoshape.getPoint().distance(pointGeoshape2.getPoint()));
//				count_edges++;
//			}
			
			time_2 = System.currentTimeMillis();
			time += (time_2-time_1);
		}
		time_4 = System.currentTimeMillis();
	    System.out.println(" Total time required for insertion of "+count_edges+" edges = "+time+" : Avg. Time = "+(time/count_edges));
		System.out.println(" Total time for whole method = "+(time_4-time_3));
	}
	
	@SuppressWarnings("unchecked")
	public static double addEdges(TitanGraph graph, String type, double distance) {
		/*
		 * Method to compute vertices in the nearby region as specified by 'distance' argument with respect to vertices of type 
		 * specified by 'type' argument and add edges between them. 
		 */
		
		int counter = 0;
		double time_1 = System.currentTimeMillis();
		for (Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
			vertex.setProperty("visible", 0);
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			/*
			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+
								vertex.getProperty("type")+" Latitude = "+latitude+ " Longitude = "+longitude+"\n");
			
			System.out.println("Vertices in "+distance+" km of locality are : ");
			*/
			// counter1 variable stores total no. of vertices satisfying Geo.WITHIN or its no. of edges
			int counter1 = 0;
			
			for (Iterator <Vertex> iterator2 = graph.query().has("type",Compare.NOT_EQUAL, type).has("visible",Compare.EQUAL,1).has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).vertices().iterator();
					iterator2.hasNext();) {
				Vertex vertex2 = iterator2.next();
//				System.out.println(counter+" : "+"Id = "+vertex2.getId()+" Place = "+vertex2.getProperty("place")+" Type = "+vertex2.getProperty("type"));
				
				//Get other point
				Geoshape pointGeoshape2 = vertex2.getProperty("place");
//				String labelString = vertex2.getProperty("type")+"-"+type;
				
//				Add edge between instances of two different types with label as type1-type2 eg:BATTERY-NARCOTICS only if NARCOTICS-BATTERY edge is not present
//			    vertex.query().has("id", vertex2.getId()).direction(Direction.BOTH).has(labelString, vertex2).vertices();
				Edge edge = vertex.addEdge(type+"-"+vertex2.getProperty("type"), vertex2);
					
//				Set property for edge as distance between two vertices
				edge.setProperty("distance",pointGeoshape.getPoint().distance(pointGeoshape2.getPoint()));
				counter1++;
			}
//			System.out.println("Vertices in nearby locality are : "+counter1+"\n");
//			System.out.println("No. of edges added are = "+counter1);			
			counter += counter1;			
		}
		double time_2 = System.currentTimeMillis();
		System.out.println("Total no. of edges added for type = "+type+" are = "+counter+" in "+(time_2-time_1)+ "ms.\n");
		return (time_2-time_1);
	}
	
	public static void addEdgesMultiThread(TitanTransaction graph1, double distance) throws InterruptedException{
		
		double time1 = System.currentTimeMillis();	

		ExecutorService executorService = Executors.newFixedThreadPool(1000);
		Iterator<Vertex> iterator = graph1.getVertices().iterator();
		int i;
		
		for(i = 1;iterator.hasNext();i++){
			
			Vertex vertex = iterator.next();
			EdgeInsertion edgeInsertion = new EdgeInsertion(graph1,vertex,distance);
			System.out.println(i);
			executorService.execute(edgeInsertion);
		}
		executorService.shutdown();
		int x = 0;
//		executorService.awaitTermination(120, TimeUnit.SECONDS);
		while(!executorService.isTerminated()){
//			System.out.println(x);
//			x++;
			;
		}
		System.out.println("All the threads terminated successfully");
		
		/*
		Iterator it = edgesMap.entrySet().iterator();
		int count = 0;
		
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        String key = (String) pairs.getKey();
	        double distance1 = (double) pairs.getValue();
	        count++;
	        
	        String[] vertex_ids = key.split("-");
	        Vertex vertex1 = graph1.getVertex(vertex_ids[0]);
	        Vertex vertex2 = graph1.getVertex(vertex_ids[1]);
	        vertex1.addEdge("knows", vertex2);
	        
//	        System.out.println("Edge added between = "+vertex_ids[0]+" and "+vertex_ids[1]);
	    }*/
		
		double time2 = System.currentTimeMillis();	
//		System.out.println("Total time required = "+(time2-time1)+ " for "+count+" edges");
		System.out.println("Total time required = "+(time2-time1));
	}
		
	public static void iterateVertices(TitanGraph graph){
		/*
		 * Iterates over all vertices of a graph and displays total no. of vertices.
		 */
		
		System.out.println("iterateVertices function called.\n");
		int counter = 0;
        	
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			Vertex vertex = iterator.next();
//			System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type")+" Visible = "+vertex.getProperty("visible"));
			counter++;
		}
		System.out.println("Total Vertices = "+counter+"\n");
	}

	public static void iterateEdges(TransactionalGraph graph) {
		/*
		 * Iterates over all edges of a graph and displays total no. of edges
		 */
		
		System.out.println("iterateEdges function called.\n");
		int counter = 0;
		
		for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
			Edge edge = iterator.next();
//			System.out.println(counter+" : "+" Edge Label = "+edge.getLabel()+" Distance = "+edge.getProperty("distance"));
			counter++;
		}
		System.out.println("Total no. of edges are = "+counter);		
	}

	public static void removeVertices(TitanGraph graph, String type) {
		/*
		 * Removes vertices of a particular type and also edges incident on it.
		 */
		for (Iterator<Vertex> iterator = graph.query().has("type", Compare.EQUAL, type).vertices().iterator();iterator.hasNext();) {
			Vertex vertex = iterator.next();
			for (Iterator<Edge> iterator2 = vertex.query().edges().iterator();iterator2.hasNext();){
				Edge edge = iterator2.next();
				edge.remove();
			}
			vertex.remove();
		}
		System.out.println("Vertices of type = "+type+" removed.");
	}

	public static void removeEdges(TitanTransaction graph){
		/*
		 * Removes all edges of the form type1-type2 and type2-type1
		 */
		System.out.println("removeEdges function called.\n");
		int counter = 0;
					
			for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
				Edge edge = iterator.next();
				edge.getVertex(Direction.IN).setProperty("visible", 1);
				edge.getVertex(Direction.OUT).setProperty("visible", 1);
				edge.remove();
				counter++;
			}
			/*
			for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator.hasNext();) {
				Vertex vertex = iterator.next();
				vertex.setProperty("visible", 1);
			}
			*/
				
		System.out.println("Total no. of edges removed were = "+counter);		
	}
	
	public static void removeEdges(TitanGraph graph, String type1, String type2) {
		String labelString ="";
		
		if (type1 == null || type2 == null) {
			labelString.concat(type1).concat(type2);
		}
	}

	public static TitanGraph exp1(Database db, TitanGraph graph, double distance) throws Exception{
		/*
		 * Need to return Titangraph instance because clearGraph functions clears the graph and returns new instance
		 * on which further processing needs to be done.
		 */
//		graph = Graph.clearGraph(db, graph);
//		Graph.InitializeGraph(graph);
		
		
//		Map<String, Integer> typeMap = Socrata.statistics(graph);
//		Graph.iterateVertices(graph);
//		Graph.removeVertices(graph, "PUBLIC INDECENCY");
//		Graph.removeVertices(graph, "NON - CRIMINAL");
		
//		Graph.iterateVertices(graph);
//		Graph.iterateEdges(graph);
//		Graph.removeEdges(graph);
		
		Graph.addEdges(graph, distance);
		/*
		double timeGeo = 0,timeEdge = 0;
		int counter = 0;
		Iterator it = typeMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        System.out.println(counter+" : Exploring Neighbours for type = " + pairs.getKey());
	        timeGeo += Graph.exploreNeighboursGeo(graph, (String) pairs.getKey(), distance);
	        timeEdge += Graph.exploreNeighboursEdge(graph, (String) pairs.getKey(), distance);
	        counter++;
	    }
	    System.out.println(counter+" : Total time required for exploring neighbours by Geo.WITHIN = "+timeGeo+" : Avg. Time = "+(timeGeo/counter));
	    System.out.println(counter+" : Total time required for exploring neighbours by Edge traversal ="+timeEdge+" : Avg. Time = "+(timeEdge/counter));
		*/
	    return graph;
	}

	public static double exploreNeighboursGeo(TitanGraph graph, String type, double distance) {
		
		System.out.println("Exploring neighbours using Geoshape and Geo.WITHIN i.e, using elasticsearch");
		int counter = 0;
		double time = 0;
		double time_1 = System.currentTimeMillis();
		
		for (Iterator<Vertex> iterator = graph.query().has("type",Compare.EQUAL, type).vertices().iterator(); iterator
				.hasNext();) {
			double time_3 = System.currentTimeMillis();
			Vertex vertex = iterator.next();
			int count = 0;
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			
			for (Iterator<Vertex> iterator2  = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator();
					iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				count++;
			}
			double time_4 = System.currentTimeMillis();
//			System.out.println("Total vertices explored = "+count);
//			System.out.println("time required for "+vertex.getId()+" = "+(time_4-time_3));
			time += time_4-time_3;
			counter+=count;
		}
		double time_2 = System.currentTimeMillis();
		
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
		System.out.print("Time excluding initial query "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
		return (time_2-time_1);
		
	}
	
	public static void exploreNeighboursGeo(TitanGraph graph, double distance) {
		
		System.out.println("Exploring neighbours using Geoshape and Geo.WITHIN i.e, using Standard Indexing or Elastic Search");
		int counter = 0;
		double time = 0;
		double time_1 = System.currentTimeMillis();
		
		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator
				.hasNext();) {
			double time_3 = System.currentTimeMillis();
			Vertex vertex = iterator.next();
			
			Geoshape pointGeoshape = vertex.getProperty("place");
			double latitude = pointGeoshape.getPoint().getLatitude();
			double longitude = pointGeoshape.getPoint().getLongitude();
			String type = vertex.getProperty("type");
//			System.out.println(type);
			

			for(Iterator<Vertex>iterator2  = graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).vertices().iterator()
					;	iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				counter++;
			}
//			counter  += graph.query().has("place", Geo.WITHIN, Geoshape.circle(latitude, longitude, distance)).has("type",Compare.NOT_EQUAL, type).
			double time_4 = System.currentTimeMillis();
			time += time_4-time_3;
//			counter++;
		}
		double time_2 = System.currentTimeMillis();
		
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
		System.out.print("Time excluding initial query "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
		
	}
	
	public static void exploreNeighboursGeoMultiThread(TitanGraph graph, double distance) {

		
		double time1 = System.currentTimeMillis();
//		Thread[] threads = new Thread[10000];
		
		ExecutorService executorService = Executors.newFixedThreadPool(100);
		Iterator<Vertex> iterator = graph.getVertices().iterator();
		int i;

		for(i = 0;iterator.hasNext();i++){
			
			Vertex vertex = iterator.next();
			ExploreNeighbours exploreNeighbours = new ExploreNeighbours(graph,vertex,distance);
			executorService.execute(exploreNeighbours);
//			threads[i] = new Thread(exploreNeighbours);
//			threads[i].start();			
		}
		executorService.shutdown();
		int count = 0;
//		System.out.println("Total vertices = "+i);
//		i--;
//		for(int j=0;j<=i;j++){
//			threads[j].join();
//		}
		
		while(!executorService.isTerminated()){
			;
		}
		
		Iterator it = countMap.entrySet().iterator();
		
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
//	        System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        count += (Integer)pairs.getValue();
	    }
		double time2 = System.currentTimeMillis();	
		System.out.println("Total time required = "+(time2-time1)+ " for "+count+" nodes");
	}
	
	public static double exploreNeighboursEdge(TitanGraph graph, String type, double distance) {
		
		System.out.println("Exploring neighbours using Edge Traversal");
		double time = 0;
		double time_1 = System.currentTimeMillis();
		int counter = 0;
		
		for (Iterator<Vertex> iterator = graph.query().has("type", Compare.EQUAL, type).vertices().iterator();iterator.hasNext();) {
//			double time_3 = System.currentTimeMillis();
		    Vertex vertex = iterator.next();
		    Iterator<Vertex> iterator2 = vertex.query().vertices().iterator();
		    /*
		     * First Method - Returns a list of vertices connected to a vertex under consideration irrespective of its distance
		     * from that vertex. 
		     */

		    /*
		    for(Iterator<Vertex> iterator2 = vertex.query().vertices().iterator();iterator2.hasNext();){
		    	Vertex vertex2 = iterator2.next();
		    	System.out.println("Vertex : "+vertex.getId()+" Vertex_In : "+vertex2.getId());
		    }
		    */
		    
		    /*
		     * Second Method - Returns a list of edges(both - OUT and IN) which can be used to filter neighbouring vertices
		     * based on distance threshold. Filtering need to be implemented. 
		     */
//		     
		    /*
		    for (Iterator<Edge> iterator2 = vertex.query().edges().iterator();iterator2.hasNext();) {
				Edge edge = iterator2.next();
				Vertex vertex2 = edge.getVertex(Direction.IN);
				System.out.println("Vertex : "+vertex.getId()+" Vertex_In : "+vertex2.getId());
			}
			*/
//		    double time_4 = System.currentTimeMillis();
//		    System.out.println("time required for "+vertex.getId()+" = "+(time_4-time_3));
//			time += time_4-time_3;
			counter++;
		    
		}
		double time_2 = System.currentTimeMillis();
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
//		System.out.print("Time excluding I/O "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
		return (time_2-time_1);
	}
	
	public static void exploreNeighboursEdge(TitanGraph graph, double distance) {
		
		System.out.println("Exploring neighbours using Edge Traversal");
		double time = 0,time_3,time_4;
		double time_1 = System.currentTimeMillis();
		int counter = 0;
		
		for (Iterator<Vertex> iterator = graph.getVertices().iterator();iterator.hasNext();) {
			time_3 = System.currentTimeMillis();
		    Vertex vertex = iterator.next();
		    Iterator<Vertex> iterator2 = vertex.query().vertices().iterator();
		    time_4 = System.currentTimeMillis(); 
		    time += time_4-time_3;
		    for( ;	iterator2.hasNext();){
				Vertex vertex2 = iterator2.next();
				counter++;
			}		    
		}
		
		double time_2 = System.currentTimeMillis();
		System.out.println("Total time = "+(time_2-time_1)+" for "+counter+" nodes");
		System.out.print("Time excluding initial query "+time +" for "+counter+" nodes and avg. time is "+(time/counter)+"\n");
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		// TODO Auto-generated method stub
		Graph.countMap = new ConcurrentHashMap<String,Integer>();
		Graph.edgesMap = new ConcurrentHashMap<String,Double>();
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		// Step 0 : Open Graph Database Connection
		Database db = new Database();
		TitanGraph graph = db.connect();
		
		System.out.println(dateFormat.format(date));
		
		// Step 1 : Clear initial graph
//		graph = clearGraph(db, graph);
			
		// Step 2 : Build Schema
//		build_schema(graph);
		
//		iterateEdges(graph);

		// Step 3 : Initialize Graph Database

		TitanTransaction graph1 = graph.newTransaction();
		
//		InitializeGraph(graph,1000);
		
//		addEdges(graph, 0.2);
//		iterateEdges(graph);		
//		graph.commit();
		
		
//		InitializeGraph(graph1,100000);
		System.out.println("Graph initialized");
		addEdgesMultiThread(graph1, 0.2);

		iterateEdges(graph1);
		graph1.commit();

		/*
		// Step 4 : Generate stats
		stats(graph);
		/*
		// Step 5 : Build edges for distance threshold = 0.2
//		addEdges(graph, 0.2);
//		iterateEdges(graph);
			
		date = new Date();
		System.out.println(dateFormat.format(date));
		
		// Step 6 : Explore neighbors for distance threshold = 0.2 using Edge Traversal
		exploreNeighboursEdge(graph, 0.2);
		
//		 Step 7 : Explore neighbors for distance threshold = 0.2 using Geo.WITHIN
//		exploreNeighboursGeo(graph,"LIQUOR LAW VIOLATION", 0.2);
		 */
		
//		Graph.countMap = new ConcurrentHashMap<String,Integer>();

//		exploreNeighboursGeo(graph, 0.2);
//		exploreNeighboursGeoMultiThread(graph, 0.2);
				
		// Step 8 : Close Graph Database Connection
//		db.close(graph);

/*		
		GremlinPipeline pipe = new GremlinPipeline();
//		pipe.start(graph.V.type.groupCount().cap());
//				.getVertex(1)).out("knows").property("name");
		pipe.start(graph.getVertices());
		pipe.aggregate();
//				type.groupCount().cap());
		System.out.println(pipe.count());
*/
		
		db.close(graph);
		
	}

}