import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonStructure;


import com.sleepycat.je.utilint.Timestamp;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class Socrata {
	
	private final String USER_AGENT = "Mozilla/5.0";
	private final int START = 0;
	private final int MAX_LIMIT = 1000;//50000;
	private final int MAX_OFFSET = 10000;//5707643;
	
	public Socrata(TitanGraph graph) throws Exception{
		
		System.out.println("Building Schema....\n");
		build_schema(graph);
		
 		System.out.println("Testing 1 - Send Http GET request\n");
 		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
 		Date date = new Date();
 		
 		int count = START;
 		long time = 0;
 		for (int i = START; i < MAX_OFFSET; i += MAX_LIMIT) {
 			long id = 0;
 			double latitude = 0, longitude = 0;
 			String type = null;
		 		
 			JsonStructure json = send_get(MAX_LIMIT,i);
 			
 			JsonArray array = (JsonArray) json;
 			long time_1 = System.currentTimeMillis();
 			
 			for (JsonValue val : array){
 				JsonObject object = (JsonObject) val;
 				count += 1;			
 		        for (String name : object.keySet()){
 		        	
 		        	if (name.equals("latitude"))
 		        	{
 		        		latitude = Double.parseDouble(object.get(name).toString());
// 		        		System.out.println(name);
// 		        		System.out.println(object.get(name)+"\n");
 		        	}
 		        	else if (name.equals("longitude"))
 	        		{
 		        		longitude = Double.parseDouble(object.get(name).toString());
// 	        			System.out.println(name);
// 	        			System.out.println(object.get(name)+"\n");
 	        		}
 		        	else if (name.equals("id"))
 	        		{
 	        			id = Long.parseLong(object.get(name).toString());
// 	        			System.out.println(name);
// 	        			System.out.println(object.get(name)+"\n");
 	        		}
 		        	else if (name.equals("primary_type"))
 	        		{
 	        			type = object.get(name).toString();
// 	        			System.out.println(name);
// 	        			System.out.println(object.get(name)+"\n");
 	        		}
 		        }
 		        
 		        Geoshape place = Geoshape.point(latitude, longitude);
 		        
 		        Vertex node = graph.addVertex(id);
 		        node.setProperty("type", type);
 		        node.setProperty("place", place);
// 		       System.out.println("Vertex added "+node.getId()+" with its data id "+id+" Total "+count+"\n");

 			}
 			long time_2 = System.currentTimeMillis();
 			time += time_2-time_1; 
// 			System.out.println("Total vertices added till now = "+count+" in "+(time_2-time_1)+" ms.");
// 			date = new Date();
// 			System.out.println(dateFormat.format(date));
		}
 		System.out.println("Total vertices added till now = "+count+" in "+time+" ms.");
	}
	
	public static void build_schema(TitanGraph graph){
		
		long time_1 = System.currentTimeMillis();
		TitanManagement mgmt = graph.getManagementSystem();
		
		PropertyKey typeKey = mgmt.makePropertyKey("type").dataType(String.class).make();
		PropertyKey placeKey = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
//		PropertyKey timeKey = mgmt.makePropertyKey("time").dataType(Timestamp.class).make();
		PropertyKey distanceKey = mgmt.makePropertyKey("distance").dataType(Double.class).make();
						
		mgmt.buildIndex("type", Vertex.class).addKey(typeKey).buildMixedIndex("search");
		mgmt.buildIndex("place", Vertex.class).addKey(placeKey).buildMixedIndex("search");
//		mgmt.buildIndex("time",Vertex.class).addKey(timeKey).buildMixedIndex("search");
		mgmt.buildIndex("distance", Edge.class).addKey(distanceKey).buildMixedIndex("search");
		
		mgmt.commit();
		long time_2 = System.currentTimeMillis();
		
		System.out.println("Schema built in "+(time_2-time_1)+" ms.");
	}
	
	@SuppressWarnings("rawtypes")
	public static Map<String, Integer> statistics(TitanGraph graph){
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
		return typeMap;
	}

	public static void navigateTree(JsonValue tree, String key) {
		   if (key != null)
		      System.out.print("Key " + key + ": ");
		   switch(tree.getValueType()) {
		      case OBJECT:
		         System.out.println("OBJECT");
		         JsonObject object = (JsonObject) tree;
		         for (String name : object.keySet())
	        		 navigateTree(object.get(name), name);
		         break;
		      case ARRAY:
		         System.out.println("ARRAY");
		         JsonArray array = (JsonArray) tree;
		         for (JsonValue val : array)
		            navigateTree(val, null);
		         break;
		      case STRING:
		         JsonString st = (JsonString) tree;
		         System.out.println("STRING " + st.getString());
		         break;
		      case NUMBER:
		         JsonNumber num = (JsonNumber) tree;
		         System.out.println("NUMBER " + num.toString());
		         break;
		      case TRUE:
		      case FALSE:
		      case NULL:
		         System.out.println(tree.getValueType().toString());
		         break;
		   }
		}
	
	// HTTP GET request
	
	public JsonStructure send_get(int limit, int offset) throws Exception {
		/**
		 * For performance, SODA APIs are paged, and return a maximum of 50,000 records per page. 
		 * So, to request subsequent pages, you’ll need to use the $limit and $offset parameters to request more data.
		 * The $limit parameter chooses how many records to return per page, and 
		 * $offset tells the API on what record to start returning data.
		 * So, to request page two, at 100 records per page, of our fuel locations API:
		 * 
		 * https://data.cityofchicago.org/resource/alternative-fuel-locations.json?$limit=100&$offset=50
		 */
		String url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit="+limit+"&$offset="+offset;
 
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GETs
		con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + con.getURL());
		System.out.println("Response Code : " + responseCode);
 
		Reader in = new InputStreamReader(con.getInputStream());
		JsonReader reader = Json.createReader(in);
		JsonStructure jsonst = reader.read();
//		System.out.println(jsonst.getClass().getName());
//		System.out.println(jsonst);
//		System.out.println(jsonst.getValueType());
		return jsonst;
				
	}
}