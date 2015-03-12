import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.sun.org.apache.xpath.internal.operations.And;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Colocation {
	
	public static Database db;
	public static MongoDatabase mongodb;
	public static TitanGraph graph;
	public static HashMap<String, Long> total_count; 
	public static double PI_threshold;
	public static boolean verbose;
	public static ConcurrentHashMap<Integer,ConcurrentHashMap<String,ConcurrentHashMap<String, Double>>> colocations;
	
	public Colocation(){
		this.db = new Database();
		this.graph = this.db.connect();
		this.total_count = new HashMap<String, Long>();
		this.PI_threshold = 0.05;
		this.verbose = false;
		this.colocations = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Double>>>();
		mongoDB mongoInstance = new mongoDB();
		this.mongodb = mongoInstance.connect();
	}	
	
	public static void print_Frequent(HashMap<String, HashMap<String, Float>> Lk, int k){
		
		System.out.println("Frequent Colocations of Size "+k+" with their participation index");
		int counter = 0;
		Iterator it,it1;
		it = Lk.entrySet().iterator();
		while(it.hasNext()){
			
			Map.Entry pair = (Map.Entry)it.next();
			String type1 = (String) pair.getKey();
			it1 = ((HashMap<String, Double>) pair.getValue()).entrySet().iterator();
			
			while(it1.hasNext()){
				
				Map.Entry pair1 = (Map.Entry)it1.next();
				String type2 = (String) pair1.getKey();
				float pi = (Float) pair1.getValue();
				
				System.out.println(type1+":"+type2+" = "+pi);
				counter++;
			}
		}
		System.out.println();
		System.out.println("Total frequent colocations of size "+k+" are "+counter);
	}
	
	public static void print_Candidate(HashSet<List<String>> Ck, int k){
		int counter = 0;
		System.out.println("Candidate Colocations of Size "+k);
		Iterator it,it1;
		it = Ck.iterator();
		while(it.hasNext()){
			List<String> tempList = ((List<String>)it.next());
			String candidate = "";
			for(int i= 0;i<tempList.size();i++){
				candidate = candidate+tempList.get(i)+":";				
			}
			candidate = candidate.substring(0, candidate.length()-1);
			System.out.println(candidate);
			counter++;
		}		
		System.out.println();
		System.out.println("Total no.of of candidates of size "+k+" are "+counter);
	}
	
	public static HashSet<List<String>> join_and_prune(HashMap<String, HashMap<String, Float>> Lk, int k){
		
		long time1 = System.currentTimeMillis();
		System.out.println("Candidate Colocations of Size "+(k+1));
		HashSet<List<String>> Ckplus1 = new HashSet<List<String>>();
		
		Iterator it,it1,it2;
		it = Lk.entrySet().iterator();
		while(it.hasNext()){
			List<String> items = new ArrayList<String>();
			String[] itemskplus1 = new String[k+1];
			
			Map.Entry pair = (Map.Entry)it.next();
			String type1 = (String) pair.getKey();
//			System.out.println("Type = "+type1);
			
			for(int i =0; i<k-1; i++){
				itemskplus1[i] = type1.split(":")[i];
//				System.out.println(type1.split(":")[i]);
			}
			
			it1 = ((HashMap<String, Double>) pair.getValue()).entrySet().iterator();
			
			while(it1.hasNext()){
				
				Map.Entry pair1 = (Map.Entry)it1.next();
				String type2 = (String) pair1.getKey();
				items.add(type2);
//				System.out.println(type2);
			}
			
			for(int i=0;i<items.size()-1;i++){
				
				for(int j=i+1; j<items.size(); j++){
					if(Integer.parseInt(items.get(i)) > Integer.parseInt(items.get(j))){
						itemskplus1[k-1] = items.get(j);
						itemskplus1[k] = items.get(i);
					}
					else{
						itemskplus1[k-1] = items.get(i);
						itemskplus1[k] = items.get(j);
					}
					
//					for(int z = 0; z < k+1;z++){
//						System.out.print(itemskplus1[z]+",");
//					}
//					System.out.println("");
					
					
					boolean flag = true;
					for(int x = 0 ; x < k+1;x++){
						
						int counter = 0;
						String[] itemsk = new String[k];
						
						for(int y = 0; y < k+1;y++){
							if(y==x){
								continue;
							}
							else {
								itemsk[counter] = itemskplus1[y];
								counter++;
							}
						}
						
						String key = "", value = "";
						int y;
						for(y = 0 ; y < k-1;y++){
							key = key+itemsk[y]+":";
						}
						value = itemsk[y];
						key = key.substring(0, key.length()-1);
//						System.out.println(key+"---"+value);
					
						if(Lk.containsKey(key)){
							HashMap<String, Float> temp = Lk.get(key);
							if(temp.containsKey(value)){
								continue;
							}else{
								flag = false;
								break;
							}
						}
						else{
							flag = false;
							break;
						}						
					
					}
					
					if(flag){
						List<String> temp_List = new ArrayList<String>();
						for(int x = 0; x<k+1;x++){
							temp_List.add(itemskplus1[x]);
						}
						Ckplus1.add(temp_List);
					}					
				}
			}
			
		}
		long time2 = System.currentTimeMillis();
		System.out.println(time1+" ---- "+time2);
		System.out.println("Total time required for joining and pruning for candidate colocations of size "+(k+1)+" is "+(time2-time1));
		return Ckplus1;
	}	
	
	public static void L1(){
		/*
		 * Generate colocations of size 1
		 * Iterate over all vertices of the graph
		 */
		System.out.println("Generating colocations of size 1.\n");
		
		long time1 = System.currentTimeMillis();
		if(verbose){
			System.out.println("Generating colocations of size 1.\n");
		}
		int counter = 0;
	    //could lead to buffer-overflow

		for (Iterator<Vertex> iterator = graph.getVertices().iterator(); iterator.hasNext();) {
				Vertex vertex = iterator.next();
				String type = vertex.getProperty("type");
				if (total_count.containsKey(type))
				{
					total_count.put(type, total_count.get(type)+1);
				}
				else {
					total_count.put(type, (long) 1);
					
				}
				if(verbose){
					System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type")+" Visible = "+vertex.getProperty("visible"));
				}
				counter++;
			}
		if(verbose){
			System.out.println("Total number of colocations of size 1 = "+counter+"\n");
		}

		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 1 : "+(time2-time1));
	}
	
	public static HashMap<String, HashMap<String, Float>> L2(){
//		HashSet<List<String>> C2
		
		/*
		 * Generate colocations of size 2
		 * Iterate over all edges of the graph
		 */
//		ConcurrentHashMap<String, ConcurrentHashMap<String,Double>> freq_C2 = new ConcurrentHashMap<String, ConcurrentHashMap<String, Double>>();
				
		//global_count 
		HashMap<String, HashMap<Long, Boolean>> global_count = new HashMap<String, HashMap<Long, Boolean>>();
		HashMap<String, HashMap<String, Float>> L2 = new HashMap<String, HashMap<String,Float>>();
		
		long time1 = System.currentTimeMillis();
		
		if(verbose){
			System.out.println("Generating colocations of size 2.\n");
		
		}
		int counter = 0;
		//could lead to buffer-overflow
		
		for (Iterator<Edge> iterator = graph.getEdges().iterator(); iterator.hasNext();) {
			Edge edge = iterator.next();
			if (verbose){
				System.out.println(counter+" : "+" Edge Label = "+edge.getLabel()+" Distance = "+edge.getProperty("distance"));
			}
			Vertex vertex1 = edge.getVertex(Direction.IN);
			String type1 = vertex1.getProperty("type");
			Long Id1 = (Long) vertex1.getId();
			Vertex vertex2 = edge.getVertex(Direction.OUT);
			String type2 = vertex2.getProperty("type");
			Long Id2 = (Long) vertex2.getId();
			//could lead to buffer-overflow	
			
			if(type1.equals(type2))
			{
				continue;
			}
			
			if(global_count.containsKey(type1+":"+type2)==false){
				HashMap<Long, Boolean> default_hashmap = new HashMap<Long, Boolean>();
				global_count.put(type1+":"+type2, default_hashmap);
			}
			if(global_count.containsKey(type2+":"+type1)==false){
				HashMap<Long, Boolean> default_hashmap = new HashMap<Long, Boolean>();
				global_count.put(type2+":"+type1, default_hashmap);
			}
			if(global_count.get(type1+":"+type2).containsKey(Id1) == false ){
				global_count.get(type1+":"+type2).put(Id1, true);
			}
			if(global_count.get(type2+":"+type1).containsKey(Id2) == false ){
				global_count.get(type2+":"+type1).put(Id2, true);
			}
			
			if(verbose){
				System.out.println("Type1 : "+type1+" Type2 : "+type2+" ID1 : "+Id1+" ID2 : "+Id2);
			}
		}
		
		Iterator it1 = global_count.entrySet().iterator();
		
		while(it1.hasNext()){
			
			Map.Entry pair1 = (Map.Entry)it1.next();
			String type = (String) pair1.getKey();
			String type1 = type.split(":")[0];
			String type2 = type.split(":")[1];
			
			if(Integer.parseInt(type1)< Integer.parseInt(type2)){
				Double x1 = (double) global_count.get(type1+":"+type2).size();
				Double x2 = (double) global_count.get(type2+":"+type1).size();
				Double a = x1/total_count.get(type1);
				Double b = x2/total_count.get(type2);
				float PI = (float) java.lang.Math.min(a, b);
				if (verbose)
				{
					System.out.println(type1);
					System.out.println(type2);
					System.out.println(x1+" / "+total_count.get(type1)+" "+type1);
					System.out.println(x2+" / "+total_count.get(type2)+" "+type2);
					System.out.println(PI);
					System.out.println("\n\n");
				}
				if(PI >= PI_threshold)
				{
					if(verbose){
						System.out.println(type1+":"+type2);
						System.out.println(PI);
						System.out.println("\n");
					}
					counter+=1;
					
					if(L2.containsKey(type1)==false){
						HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
						tempHashMap.put(type2, PI);
						L2.put(type1, tempHashMap);
					}
					else{
						L2.get(type1).put(type2, PI);
					}					
				}
			}
			
		}
		
		if (verbose){
			System.out.println("Total no. of colocations of size 2  are = "+counter);
		}
		
		/*
		HashSet<List<String>> C3 = new HashSet<List<String>>();
		
//		HashMap<String, HashSet<String>> temp_C3 = new HashMap<String, HashSet<String>>();
		
		it1 = temp_C3.entrySet().iterator();
		Iterator it2, it3;
		
		String string1, string2, string3;
		
		it1 = temp_C3.entrySet().iterator();
		while(it1.hasNext()){
			Map.Entry pair1 = (Map.Entry)it1.next();
			string1 = (String) pair1.getKey(); 
			it2 = ((Iterable) pair1.getValue()).iterator();
			while(it2.hasNext()){
				string2 = (String) it2.next();
				
				if (Integer.parseInt(string1) >= Integer.parseInt(string2)  )
				{
					continue;
				}
				it3 = temp_C3.get(string2).iterator();
				while(it3.hasNext()){
					string3 = (String) it3.next();
					if (Integer.parseInt(string2) >= Integer.parseInt(string3)  )
					{
						continue;
					}
					if(temp_C3.get(string1).contains(string3))
					{
						if (verbose){
							System.out.println("\n");
							System.out.println(string1);
							System.out.println(string2);
							System.out.println(string3);
							System.out.println("\n");
						}
						List<String> temp_list = new ArrayList();
						temp_list.add(string1);
						temp_list.add(string2);
						temp_list.add(string3);
						C3.add(temp_list);
					}
				}
			}
		}
		*/
		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 2 : "+(time2-time1));
//		return C3;
		return L2;
		
	}
	
	public static HashMap<String, HashMap<String, Float>> L3(HashSet<List<String>> Ck, int k){
		/*
		 * Generate colocation of size 3
		 */
		long time1 = System.currentTimeMillis();
		
		HashMap<String, HashMap<String, Float>> Lk = new HashMap<String, HashMap<String,Float>>();
		System.out.println("Generating Colocations of Size "+k);
		Iterator it = Ck.iterator();
		
		while(it.hasNext()){
			int total_cliques = 0;
			
			HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
			
//			System.out.println("Validating Following colocation");
			
			List<String> tempList = (List<String>) it.next();
			for(int i=0;i<tempList.size();i++){
				String key = tempList.get(i);
				if(unique.containsKey(key)==false){
					HashSet<Long> tempHashSet = new HashSet<Long>();
					unique.put(key, tempHashSet);
				}
			}
			String type1 = tempList.get(0);
			String type2 = tempList.get(1);
			String type3 = tempList.get(2);
//			System.out.println(type1+":"+type2+":"+type3);
			
			// Initialize the collection A:B:C
			MongoCollection<Document> coll = mongodb.getCollection(type1+":"+type2+":"+type3);

					
			Iterator<Vertex> it1 = graph.query().has("type", Compare.EQUAL, type1).vertices().iterator();
			while(it1.hasNext()){
				
				Vertex vertex1 = it1.next();
				long id1 = (long) vertex1.getId();
				unique.get(type1).add(id1);
				
				Iterator<Vertex> it2 = vertex1.getVertices(Direction.OUT, type1+"-"+type2).iterator();
				Iterator<Vertex> it3 = vertex1.getVertices(Direction.OUT, type1+"-"+type3).iterator();
 				
				while (it2.hasNext()) {
					Vertex vertex2 = (Vertex) it2.next();
					long id2 = (long) vertex2.getId();
					
					while (it3.hasNext()) {
						Vertex vertex3 = (Vertex) it3.next();
						long id3 = (long) vertex3.getId();
						
						if(areConnected(vertex2, vertex3)){
							
							if(unique.get(type1).contains(id1)==false){
								unique.get(type1).add(id1);
							}
							if(unique.get(type2).contains(id2)==false){
								unique.get(type2).add(id2);
							}
							if(unique.get(type3).contains(id3)==false){
								unique.get(type3).add(id3);
							}
							
							BasicDBObject query = new BasicDBObject(id1+":"+id2, new BasicDBObject("$exists",true));
							if(coll.find(query).first()!=null){
								Document result = coll.find(query).first();
								Document documents = (Document) result.get(id1+":"+id2);
								if(documents.containsKey(""+id3)==false){
									documents.put(""+id3, 1);
									coll.replaceOne(query, result);							
								}
							}else{
//								System.out.println(i+":"+j);
								coll.insertOne(new Document(id1+":"+id2, new Document(""+id3,1)));
							}
							
							total_cliques++;
						}
					}
				}
			}
			float count_type1 = total_count.get(type1);
			float count_type2 = total_count.get(type2);
			float count_type3 = total_count.get(type3);
			float pr_type1 = ((float) unique.get(type1).size())/count_type1;
			float pr_type2 = ((float) unique.get(type2).size())/count_type2;
			float pr_type3 = ((float) unique.get(type3).size())/count_type3;
			float pi = pr_type1;
			if(pr_type2 < pi){
				if(pr_type3 < pi)
				{
					pi = pr_type3;
				}else{
					pi = pr_type2;
				}
			}else{
				if(pr_type3 < pi){
					pi = pr_type3;
				}
			}
			if(pi>PI_threshold){
				System.out.println("Frequent : "+type1+":"+type2+":"+type3+" PI = "+pi);
				
				System.out.println("Total Count = "+coll.count()+" Total cliques are = "+total_cliques);

//				MongoCursor<Document> cursor = coll.find().iterator();
//				try {
//				    while (cursor.hasNext()) {
//				        System.out.println(cursor.next());
//				    }
//				} finally {
//				    cursor.close();
//				}
				
				
				if(Lk.containsKey(type1+":"+type2)==false){
					HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
					tempHashMap.put(type3, pi);
					Lk.put(type1+":"+type2, tempHashMap);
				}else{
					Lk.get(type1+":"+type2).put(type3, pi);
				}
			}
			else{
//				System.out.println("In-Frequent : "+type1+":"+type2+":"+type3+" PI = "+pi);
				coll.dropCollection();
			}
		}
		long time2 = System.currentTimeMillis();
		System.out.println("Total time for verifying itemsets of size "+k+" = "+(time2-time1));
		return Lk;
	}
	
	public static boolean areConnected(Vertex vertex1, Vertex vertex2){
		String type1 = vertex1.getProperty("type");
		String type2 = vertex2.getProperty("type");
		long id1 = (long) vertex1.getId();
		long id2 = (long) vertex2.getId();
		
		if(Integer.parseInt(type1) < Integer.parseInt(type2)){
			for(Iterator<Vertex> it = vertex1.getVertices(Direction.OUT,type1+"-"+type2).iterator();
					it.hasNext();){
				Vertex vertex3 = it.next();
//				System.out.println(type+"-"+type2);
				if(id2 == ((long)vertex3.getId())){					
					return true;
				}
			}
		}
		if(Integer.parseInt(type1) > Integer.parseInt(type2))
		{			
			for(Iterator<Vertex> it = vertex2.getVertices(Direction.OUT,type2+"-"+type1).iterator();
					it.hasNext();){
				Vertex vertex3 = it.next();
//				System.out.println(type2+"-"+type);
				if(id1 == ((long)vertex3.getId())){					
					return true;
				}
			}
		}
		
		return false;
	}
	
	public static boolean areConnected(long id1, long id2) {
		
		Vertex vertex = graph.getVertex(id1);
		String type = vertex.getProperty("type");
		Vertex vertex2 = graph.getVertex(id2);
		String type2 = vertex2.getProperty("type");

		if(Integer.parseInt(type) < Integer.parseInt(type2)){
			for(Iterator<Vertex> it = vertex.getVertices(Direction.OUT,type+"-"+type2).iterator();
					it.hasNext();){
				Vertex vertex3 = it.next();
//				System.out.println(type+"-"+type2);
				if(id2 == ((long)vertex3.getId())){					
					return true;
				}
			}
		}
		if(Integer.parseInt(type) > Integer.parseInt(type2))
		{			
			for(Iterator<Vertex> it = vertex2.getVertices(Direction.OUT,type2+"-"+type).iterator();
					it.hasNext();){
				Vertex vertex3 = it.next();
//				System.out.println(type2+"-"+type);
				if(id1 == ((long)vertex3.getId())){					
					return true;
				}
			}
		}
		return false;
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		Colocation colocation = new Colocation();
		// Total count of all size-1 colocations
		L1();
//		
//		// Frequent Colocations of size 2
		HashMap<String, HashMap<String, Float>> L2 = L2();
		print_Frequent(L2, 2);
//		
//		// Candidate Colocations of size 3
		HashSet<List<String>> C3 = join_and_prune(L2, 2);
		print_Candidate(C3, 3);
//		
		HashMap<String, HashMap<String, Float>> L3 = L3(C3, 3);
		print_Frequent(L3, 3);
//		
		HashSet<List<String>> C4 = join_and_prune(L3, 3);
		print_Candidate(C4, 4);
		
		// Code Snippet for MongoDB
		/*
		MongoCollection<Document> coll = mongodb.getCollection("A:B:C");
		
		for(int i=0;i<5;i++){
			for(int j = 0;j<5;j++){
				BasicDBObject query = new BasicDBObject(i+":"+j, new BasicDBObject("$exists",true));
				
//				Document result = coll.find(query).first();
//				System.out.println(result+"            "+result.get("_id"));
//				Document documents = (Document) result.get(i+":"+j);
//				System.out.println(documents);

				for(int k = 0;k<10;k++){
					if(coll.find(query).first()!=null){
						Document result = coll.find(query).first();
						Document documents = (Document) result.get(i+":"+j);
						if(documents.containsKey(""+k)==false){
							documents.put(""+k, 1);
							coll.replaceOne(query, result);							
						}
					}else{
						System.out.println(i+":"+j);
						coll.insertOne(new Document(i+":"+j, new Document(""+k,1)));
//						coll.replaceOne(query, result);
					}					
				}
//				System.out.println(documents);
//				System.out.println(result);
			}
		}
		System.out.println("Total Count = "+coll.count());

		MongoCursor<Document> cursor = coll.find().iterator();
		try {
		    while (cursor.hasNext()) {
		        System.out.println(cursor.next());
		    }
		} finally {
		    cursor.close();
		}
		
		coll.dropCollection();
		*/
		
		
		// Code to verify that areConnected() method is correctly
		/*
		List<Long> idsList = new ArrayList<Long>();
		for(Iterator<Vertex> it = graph.getVertices().iterator(); it.hasNext();){
			Vertex vertex = it.next();
			idsList.add((long)vertex.getId());
		}
		
		int counter = 0;
		System.out.println("Total vertices = "+idsList.size());
		
		for(int i = 0;i<idsList.size()-1;i++){
			for(int j = i+1;j<idsList.size();j++){
				if(areConnected(idsList.get(i), idsList.get(j))==true){
					counter++;
				}
			}
		}
		System.out.println("Total lookups are = "+counter);
		*/
		
		db.close(graph);
		
	}
}
