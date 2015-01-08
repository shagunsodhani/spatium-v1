import java.awt.print.Printable;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import javax.net.ssl.HttpsURLConnection;
import javax.json.JsonValue;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import org.json.JSONArray;


public class Socrata {
	
	private final String USER_AGENT = "Mozilla/5.0";
	
	public static void main(String[] args) throws Exception {
		 
		Socrata http = new Socrata();
 		System.out.println("Testing 1 - Send Http GET request");
 		String latitude, longitude, id, type;
 		
 		
		JsonStructure json = http.send_get();
		
		JsonArray array = (JsonArray) json;
		for (JsonValue val : array){
			JsonObject object = (JsonObject) val;
	        for (String name : object.keySet()){
	        	
	        	if (name.equals("latitude"))
	        	{
	        		latitude = object.get(name).toString();
	        		System.out.println(name);
	        		System.out.println(object.get(name));
	        	}
	        	else if (name.equals("longitude"))
        		{
	        		longitude = object.get(name).toString();
        			System.out.println(name);
//        			System.out.println(array);
        			System.out.println(object.get(name));
        		}
	        	else if (name.equals("id"))
        		{
        			id = object.get(name).toString();
        			System.out.println(name);
        			System.out.println(object.get(name));
        		}
	        	else if (name.equals("primary_type"))
        		{
        			type = object.get(name).toString();
        			System.out.println(name);
        			System.out.println(object.get(name));
        		}
	        }
			
		}
			
	         
//		id: STRING 9338511
//		longitude: STRING -87.665629386
//		Key latitude: STRING 41.781663841
//		Key primary_type: STRING ROBBERY
//		
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
	private JsonStructure send_get() throws Exception {
		/**
		 * For performance, SODA APIs are paged, and return a maximum of 50,000 records per page. So, to request subsequent pages, you’ll need to use the $limit and $offset parameters to request more data. The $limit parameter chooses how many records to return per page, and $offset tells the API on what record to start returning data.

So, to request page two, at 100 records per page, of our fuel locations API: 

 https://data.cityofchicago.org/resource/alternative-fuel-locations.json?$limit=100&$offset=50
		 */
		String url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=2&$offset=0 ";
 
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