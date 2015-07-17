import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;

public class Colocation {
	
	public static Database db;
	public static MongoDatabase mongodb;
	public static TitanGraph graph;
	public static HashMap<String, Long> total_count; 
	public static double PI_threshold;
	public static boolean verbose;
	public static ConcurrentHashMap<List<String>,Float>  colocations;
	public static MongoClient mongoClient;
	
	public Colocation(){
		this.db = Database.getInstance();
		this.graph = this.db.getTitanGraph();
		this.total_count = new HashMap<String, Long>();
		this.PI_threshold = 0.1;
		this.verbose = false;
		this.colocations = new ConcurrentHashMap<List<String>,Float>();
		MongoDB mongoInstance = new MongoDB();
		this.mongoClient = mongoInstance.getMongoClient();
		this.mongodb = mongoInstance.getMongoDatabase(true);
	}
	
	public Colocation(Database db, TitanGraph graph){
		this.db = db;
		this.graph = graph;
		this.total_count = new HashMap<String, Long>();
		this.PI_threshold = 0.001;
		this.verbose = false;
		this.colocations = new ConcurrentHashMap<List<String>,Float>();
		MongoDB mongoInstance = new MongoDB();
		this.mongoClient = mongoInstance.getMongoClient();
		this.mongodb = mongoInstance.getMongoDatabase(true);
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
//				System.out.println(type1+":"+type2+" = "+pi);
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
//			System.out.println(candidate);
			counter++;
		}		
		System.out.println();
		System.out.println("Total no.of of candidates of size "+k+" are "+counter);
	}
	
	public static HashSet<List<String>> join_and_prune(HashMap<String, HashMap<String, Float>> Lk, int k){
		
		long time1 = System.currentTimeMillis();
		System.out.println("Joining and Pruning to get Candidate Colocations of Size "+(k+1));
		HashSet<List<String>> Ckplus1 = new HashSet<List<String>>();
		
		Iterator it,it1,it2;
		it = Lk.entrySet().iterator();
		int candidate_count = 0;
		
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
						candidate_count++;
					}					
				}
			}
			
		}
		long time2 = System.currentTimeMillis();
//		System.out.println(time1+" ---- "+time2);
		System.out.println("Total time required for joining and pruning for candidate colocations of size "+(k+1)+" is "+(time2-time1));
		System.out.println("Total Candidates of size "+(k+1)+" are "+candidate_count);
		return Ckplus1;
	}	
	
	public static HashSet<List<String>> L1(){
		/*
		 * Generate colocations of size 1
		 * Iterate over all vertices of the graph
		 */
		System.out.println("Generating colocations of size 1.\n");
		HashSet<List<String>> Ckplus1 = new HashSet<List<String>>();
		List<String> items = new ArrayList<String>();
				
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
					items.add(type);
					
				}
				if(verbose){
					System.out.println(counter+" : "+"Id = "+vertex.getId()+" Place = "+vertex.getProperty("place")+" Type = "+vertex.getProperty("type")+" Visible = "+vertex.getProperty("visible"));
				}
				counter++;
			}
		if(true){
			System.out.println("Total number of colocations of size 1 = "+total_count.size());
		}

		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 1 : "+(time2-time1));
		for(int i = 0; i<items.size()-1;i++){
			for(int j = i+1; j<items.size();j++){
				List<String> temp_List = new ArrayList<String>();
				if (Integer.parseInt(items.get(i)) < Integer.parseInt(items.get(j))) {
					temp_List.add(items.get(i));
					temp_List.add(items.get(j));					
				}else{
					temp_List.add(items.get(j));
					temp_List.add(items.get(i));
				}
				Ckplus1.add(temp_List);
			}
		}
		
		return Ckplus1;
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
		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 2 : "+(time2-time1));
		return L2;
		
	}
	
	public static HashMap<String, HashMap<String, Float>> multithreaded_L2(HashSet<List<String>> Ck, int k, boolean create_db){
		
		long time1 = System.currentTimeMillis();
		HashMap<String, HashMap<String, Float>> L2 = new HashMap<String, HashMap<String,Float>>();
		System.out.println("Generating Colocations of Size "+k);
		Iterator it = Ck.iterator();
		ExecutorService executorService = Executors.newFixedThreadPool(48);
		
		while(it.hasNext()){
			ValidateCandidate validateCandidate = new ValidateCandidate((List<String>) it.next(), k, create_db);
			executorService.execute(validateCandidate);
		}
		executorService.shutdown();
//		executorService.awaitTermination(120, TimeUnit.SECONDS);
		while(!executorService.isTerminated()){
			;
		}
		System.out.println("All the threads terminated successfully");
		it = colocations.entrySet().iterator();
		
		while(it.hasNext()){
			Map.Entry<List<String>, Float> pairs = (Map.Entry<List<String>, Float>) it.next();
			List<String> tempList = (List<String>) pairs.getKey();
			String type1 = tempList.get(0);
			String type2 = tempList.get(1);
			
			if(L2.containsKey(type1)==false){
				HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
				tempHashMap.put(type2, pairs.getValue());
				L2.put(type1, tempHashMap);
			}
			else{
				L2.get(type1).put(type2, pairs.getValue());
			}	
		}
		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 2 : "+(time2-time1));
		colocations.clear();
		return L2;
	}

	public static HashMap<String, HashMap<String,Float>> L3(HashSet<List<String>> Ck, int k, boolean create_db){
		/*
		 * Generate colocation of size 3
		 */
		
		if(Ck.isEmpty()){
			System.out.println("Yes Empty");
		}
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
			
			String dbname1 = type1+":"+type2+":"+type3;
			MongoCollection<Document> coll ;						
			if (create_db==true){
				
//				mongoDB new_mongoInstance = new mongoDB(type1+":"+type2+":"+type3);
				MongoDB new_mongoInstance = new MongoDB(dbname1);
				MongoDatabase new_mongodb = new_mongoInstance.getMongoDatabase(false);
//				coll = new_mongodb.getCollection(type1+":"+type2+":"+type3);
				coll = new_mongodb.getCollection(dbname1);
			}
			// Initialize the collection A:B:C
			else{
//				coll = mongodb.getCollection(type1+":"+type2+":"+type3);
				coll = mongodb.getCollection(dbname1);
			}
					
			Iterator<Vertex> it1 = graph.query().has("type", Compare.EQUAL, type1).vertices().iterator();
			while(it1.hasNext()){
				
				Vertex vertex1 = it1.next();
				long id1 = (long) vertex1.getId();
								
				List<Long> v2 = new ArrayList<Long>();
				List<Long> v3 = new ArrayList<Long>();
				boolean flag_outer = false;
				
//				Iterator<Edge> it2 = vertex1.getEdges(Direction.IN, type1+"-"+type2).iterator();
				Iterator<Vertex> it2 = vertex1.getVertices(Direction.IN, type1+"-"+type2).iterator();
				while(it2.hasNext()){
//					Edge edge = it2.next();
//					Vertex vertex2 = edge.getVertex(Direction.OUT);
					Vertex vertex2 = it2.next();
					v2.add((long) vertex2.getProperty("id"));
				}
				
//				Iterator<Edge> it3 = vertex1.getEdges(Direction.IN, type1+"-"+type3).iterator();
				Iterator<Vertex> it3 = vertex1.getVertices(Direction.IN, type1+"-"+type3).iterator();
				while(it3.hasNext()){
//					Edge edge = it3.next();
//					Vertex vertex3 = edge.getVertex(Direction.OUT);
					Vertex vertex3 = it3.next();
					v3.add((long) vertex3.getProperty("id"));
				}
				for(int i =0 ; i < v2.size();i++){
					boolean flag_inner = false;
					List<Long> temp_list = new ArrayList<Long>();

					for(int j =0; j < v3.size(); j++){
							if(areConnected(v2.get(i), v3.get(j))==true){
							unique.get(type3).add(v3.get(j));
							temp_list.add(v3.get(j));
							flag_inner = true;
							total_cliques++;
						}
					}
					if(flag_inner == true){
						unique.get(type2).add((v2.get(i)));
						flag_outer = true;
						coll.insertOne(new Document("value", temp_list).append("key", id1+":"+v2.get(i)));
					}
				}
				if(flag_outer == true){
					unique.get(type1).add(id1);
				}
				
				
				/*
				Iterator<Edge> it2 = vertex1.getEdges(Direction.IN, type1+"-"+type2).iterator();
//				Iterator<Vertex> it2 = vertex1.getVertices(Direction.IN, type1+"-"+type2).iterator();
//				Iterator<Vertex> it3 = vertex1.getVertices(Direction.IN, type1+"-"+type3).iterator();

				while (it2.hasNext()) {
					Edge e2 = it2.next();
//					Vertex vertex2 = (Vertex) it2.next();
					Vertex vertex2 = (Vertex) e2.getVertex(Direction.OUT);
					long id2 = (long) vertex2.getId();
					
					Iterator<Edge> it3 = vertex1.getEdges(Direction.IN, type1+"-"+type3).iterator();
					while (it3.hasNext()) {
						Edge e3 = it3.next();
//						Vertex vertex3 = (Vertex) it3.next();
						Vertex vertex3 = (Vertex) e3.getVertex(Direction.OUT);
						long id3 = (long) vertex3.getId();
						
						if(areConnected(id2, id3)==true){
							
							unique.get(type1).add(id1);
							unique.get(type2).add(id2);
							unique.get(type3).add(id3);

							BasicDBObject searchQuery = new BasicDBObject().append("key", id1+":"+id2);
							if(coll.find(searchQuery).first()!=null){
								Document result = coll.find(searchQuery).first();
								List<Long> documents = (List<Long>) result.get("value");
								documents.add(id3);
								coll.replaceOne(searchQuery, result);
							}else{
								List<Long> tempList2 = new ArrayList<Long>();
								tempList2.add(id3);
								coll.insertOne(new Document("value", tempList2).append("key", id1+":"+id2));
							}
							total_cliques++;
						}
					}
				}
				*/
				
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
			
			if(pi>=PI_threshold){
				if(verbose){
					System.out.println("--------------");
					System.out.println("Frequent = "+type1+":"+type2+":"+type3+" PI = "+pi);
					System.out.println("Unique_Count = "+unique.get(type1).size()+":"+unique.get(type2).size()+":"+unique.get(type3).size());
					System.out.println("Total_Count = "+count_type1+":"+count_type2+":"+count_type3);
					System.out.println("Total Count = "+coll.count()+" Total cliques are = "+total_cliques);
				}
				if(Lk.containsKey(type1+":"+type2)==false){
					HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
					tempHashMap.put(type3, pi);
					Lk.put(type1+":"+type2, tempHashMap);
				}else{
					Lk.get(type1+":"+type2).put(type3, pi);
				}
			}
			else{
				coll.dropCollection();
				if(create_db==true){
					mongoClient.dropDatabase(dbname1);
//					mongoClient.dropDatabase(type1+":"+type2+":"+type3);
				}
			}
		}
		long time2 = System.currentTimeMillis();
		System.out.println("Total time for verifying itemsets of size "+k+" = "+(time2-time1));
		return Lk;
	}
		
	public static HashMap<String, HashMap<String, Float>> multithreaded_L3(HashSet<List<String>> Ck, int k, boolean create_db){
		/*
		 * Generate colocation of size 3
		 */
		long time1 = System.currentTimeMillis();
		
		HashMap<String, HashMap<String, Float>> Lk = new HashMap<String, HashMap<String,Float>>();
		System.out.println("Generating Colocations of Size "+k);
		Iterator iterator = Ck.iterator();
		int count = 0;
		while (iterator.hasNext()) {
			iterator.next();
			count++;			
		}
		ExecutorService executorService = null;
		if(count<48){
			executorService = Executors.newFixedThreadPool(count);
		}
		else{
			executorService = Executors.newFixedThreadPool(48);
		}
		
		Iterator it = Ck.iterator();
		
		while(it.hasNext()){
			ValidateCandidate validateCandidate = new ValidateCandidate((List<String>) it.next(), k, create_db);
			executorService.execute(validateCandidate);
		}
		executorService.shutdown();
//		executorService.awaitTermination(120, TimeUnit.SECONDS);
		while(!executorService.isTerminated()){
			;
		}
		System.out.println("All the threads terminated successfully");
		
//		this.colocations = new ConcurrentHashMap<List<String>,Float>();
		
		it = colocations.entrySet().iterator();
		
				
		while(it.hasNext()){
			Map.Entry<List<String>, Float> pairs = (Map.Entry<List<String>, Float>) it.next();
			List<String> tempList = (List<String>) pairs.getKey();
			String type1 = "";
			int i, j;
			for(i = 0; i < k-2; i++){
				type1 += tempList.get(i)+":";
			}
			type1 = type1.substring(0, type1.length()-1);
			String type2 = tempList.get(k-2);
			String type3 = tempList.get(k-1);
			
			if(Lk.containsKey(type1+":"+type2)==false){
				HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
				tempHashMap.put(type3, pairs.getValue());
				Lk.put(type1+":"+type2, tempHashMap);
			}else{
				Lk.get(type1+":"+type2).put(type3, pairs.getValue());
			}			
		}
		colocations.clear();
		long time2 = System.currentTimeMillis();
		System.out.println("Total time for verifying itemsets of size "+k+" = "+(time2-time1));
		return Lk;
	}
	
	public static HashMap<String, HashMap<String, Float>> Lk(HashSet<List<String>> Ck, int k, boolean create_db){
		
		long time1 = System.currentTimeMillis();
		HashMap<String, HashMap<String, Float>> Lk = new HashMap<String, HashMap<String,Float>>();
		System.out.println("Generating Colocations of Size "+k);
		
		Iterator it = Ck.iterator();
		
		while(it.hasNext()){
			HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
			
			List<String> tempList = (List<String>) it.next();
			for(int i=0;i<tempList.size();i++){
				String key = tempList.get(i);
				if(unique.containsKey(key)==false){
					HashSet<Long> tempHashSet = new HashSet<Long>();
					unique.put(key, tempHashSet);
				}
			}
			String type1 = "";
			int i, j;
			for(i = 0; i < k-2; i++){
				type1 += tempList.get(i)+":";
			}
			type1 = type1.substring(0, type1.length()-1);
			String type2 = tempList.get(k-2);
			String type3 = tempList.get(k-1);
			
			System.out.println(type1+":"+type2+":"+type3);
			
			// Initilize the collection A:B:C...k-terms
			
			
			MongoCollection<Document> coll ;
			MongoCollection<Document> coll_1 ;
			MongoCollection<Document> coll_2 ;
			
			String dbname1, dbname2, dbname3;
			dbname1 = type1+":"+type2+":"+type3;
			dbname2 = type1+":"+type2;
			dbname3 = type1+":"+type3;
					
			if (create_db==true){
//				mongoDB new_mongoInstance = new mongoDB(type1+":"+type2+":"+type3);
//				mongoDB new_mongoInstance_1 = new mongoDB(type1+":"+type2);
//				mongoDB new_mongoInstance_2 = new mongoDB(type1+":"+type3);
				
				MongoDB new_mongoInstance = new MongoDB(dbname1);
				MongoDB new_mongoInstance_1 = new MongoDB(dbname2);
				MongoDB new_mongoInstance_2 = new MongoDB(dbname3);
		

				MongoDatabase new_mongodb = new_mongoInstance.getMongoDatabase(false);
				MongoDatabase new_mongodb_1 = new_mongoInstance_1.getMongoDatabase(false);
				MongoDatabase new_mongodb_2 = new_mongoInstance_2.getMongoDatabase(false);
				
//				coll = new_mongodb.getCollection(type1+":"+type2+":"+type3);
//				coll_1 = new_mongodb_1.getCollection(type1+":"+type2);
//				coll_2 = new_mongodb_2.getCollection(type1+":"+type3);
				
				coll = new_mongodb.getCollection(dbname1);
				coll_1 = new_mongodb_1.getCollection(dbname2);
				coll_2 = new_mongodb_2.getCollection(dbname3);
				
			}
			// Initialize the collection A:B:C
			else{
//				coll = mongodb.getCollection(type1+":"+type2+":"+type3);
//				coll_1 = mongodb.getCollection(type1+":"+type2);
//				coll_2 = mongodb.getCollection(type1+":"+type3);
				
				coll = mongodb.getCollection(dbname1);
				coll_1 = mongodb.getCollection(dbname2);
				coll_2 = mongodb.getCollection(dbname3);
			}
			
//			MongoCollection<Document> coll = mongodb.getCollection(type1+":"+type2+":"+type3);
//			MongoCollection<Document> coll_1 = mongodb.getCollection(type1+":"+type2);
//			MongoCollection<Document> coll_2 = mongodb.getCollection(type1+":"+type3);
			
			MongoCursor<Document> cursor_1 = coll_1.find().iterator();
			while (cursor_1.hasNext()) {
					Document doc_1 = cursor_1.next();
					// id1 is a string delimited by :
					String id1ist = doc_1.getString("key");
					BasicDBObject searchQuery = new BasicDBObject().append("key", id1ist);
					
					if(coll_2.find(searchQuery).first()!=null){
						
						Document doc_2 = coll_2.find(searchQuery).first();
						List<Long> doc_1_value =  (List<Long>) doc_1.get("value");
						List<Long> doc_2_value =  (List<Long>) doc_2.get("value");
						boolean flag_outer = false;
						for (i = 0; i < doc_1_value.size(); i++) {
							boolean flag_inner = false;
							List<Long> temp_list = new ArrayList<Long>();
							for (j=0; j<doc_2_value.size(); j++){
								if(areConnected(doc_1_value.get(i), doc_2_value.get(j))){
//									System.out.println(i+" : "+j);
									flag_inner = true;
									unique.get(type3).add((doc_2_value.get(j)));
									temp_list.add(doc_2_value.get(j));
								}	
							}
							if(flag_inner == true){
								unique.get(type2).add((doc_1_value.get(i)));
								flag_outer = true;
								coll.insertOne(new Document("value", temp_list).append("key", id1ist+":"+doc_1_value.get(i)));
							}
						}
						if(flag_outer ==  true){
							for(int x = 0; x < k-2; x++){
								unique.get(tempList.get(x)).add(Long.parseLong(id1ist.split(":")[x]));
							}
						}
					}
					
//			        System.out.println(cursor_1.next());
			}
			
			float ParticipationIndex = (float)1.0;
			for (int x = 0; x < tempList.size(); x++) {
				float ParticipationRatio = unique.get(tempList.get(x)).size()/((float)total_count.get(tempList.get(x))); 
				if(ParticipationIndex > ParticipationRatio)
				{
					ParticipationIndex = ParticipationRatio;
				}
			}
			if(ParticipationIndex < PI_threshold){
				
				if(create_db==true){
//					mongoClient.dropDatabase(type1+":"+type2+":"+type3);
//					mongoClient.dropDatabase(type1+":"+type2);
//					mongoClient.dropDatabase(type1+":"+type3);
				
					mongoClient.dropDatabase(dbname1);
				}
				else{
					coll.dropCollection();
				}
			}
			else{
//				System.out.println(type1+":"+type2+":"+type3+" = "+ParticipationIndex);
				if(Lk.containsKey(type1+":"+type2)==false){
					HashMap<String, Float> tempHashMap = new HashMap<String, Float>();
					tempHashMap.put(type3, ParticipationIndex);
					Lk.put(type1+":"+type2, tempHashMap);
				}else{
					Lk.get(type1+":"+type2).put(type3, ParticipationIndex);
				}
			}			
			
//			System.out.println(type1+":"+type2+" = "+coll_1.count());
//			System.out.println(type1+":"+type3+" = "+coll_2.count());
						
		}		
		
		long time2 = System.currentTimeMillis();
		System.out.println("Total time required to get frequent colocations of size "+k+" = "+(time2-time1));
		return Lk;
	}
	
	public static HashMap<String, HashMap<String, Float>> multithreaded_Lk(HashSet<List<String>> Ck, int k, boolean create_db){
		
		long time1 = System.currentTimeMillis();
		HashMap<String, HashMap<String, Float>> Lk = new HashMap<String, HashMap<String,Float>>();
		System.out.println("Generating Colocations of Size "+k);
		
		Iterator it = Ck.iterator();
		while(it.hasNext()){
			
		}		
		
		long time2 = System.currentTimeMillis();
		System.out.println("Total time required to get frequent colocations of size "+k+" = "+(time2-time1));
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
			for(Iterator<Vertex> it = vertex.getVertices(Direction.IN,type+"-"+type2).iterator();
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
			for(Iterator<Vertex> it = vertex2.getVertices(Direction.IN,type2+"-"+type).iterator();
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
	
	public static boolean areConnected(long id1, final long id2, String label) {
		
		GremlinPipeline pipe = new GremlinPipeline();
		pipe.start(graph.getVertex(id1)).in(label).filter(new PipeFunction<Vertex, Boolean>() {
			public Boolean compute(Vertex argument){
				if((Long)argument.getId() == id2){
					return true;
				}
				else{
					return false;
				}
			}
		}).path(new PipeFunction<Vertex, Long>(){
			public Long compute(Vertex argument) {
				return (Long) argument.getId();
			}
		}).enablePath();
				
		if(pipe.hasNext()){
//			System.out.println(pipe);
			return true;
		}else{
			return false;
		}
		
	}
	
	public static void verify_areConnected(){
		// Code to verify that areConnected() method is correctly working
//		MongoCollection<Document> coll = mongodb.getCollection("Id_latlong");
//
//		List<Long> idsList = new ArrayList<Long>();
//		for(Iterator<Vertex> it = graph.getVertices().iterator(); it.hasNext();){
//			Vertex vertex = it.next();
//			idsList.add((long)vertex.getId());
//			Document doc = new Document("id",vertex.getId()).append("lat", ((Geoshape)vertex.getProperty("place")).getPoint().getLatitude()).append("long", ((Geoshape)vertex.getProperty("place")).getPoint().getLongitude());
//			coll.insertOne(doc);
//		}
//				
//		int counter = 0;
//		System.out.println("Total vertices = "+idsList.size());
//		System.out.println("Total lookups are = "+counter);
		
		long time1 = System.currentTimeMillis();

		List<Long> idsList = new ArrayList<Long>();
		List<String> label = new ArrayList<String>();
		
 		for(Iterator<Vertex> it = graph.getVertices().iterator(); it.hasNext();){
 			Vertex vertex = it.next();
 			idsList.add((long)vertex.getId());
 			label.add((String)vertex.getProperty("type"));
 		}
 		
 		int counter = 0;
 		System.out.println("Total vertices = "+idsList.size());
 		
 		for(int i = 0;i<idsList.size();i++){
 			for(int j = 0;j<idsList.size();j++){
 				if(i==j)
 					continue;
 				
 				if(areConnected(idsList.get(i), idsList.get(j),label.get(i)+"-"+label.get(j))==true){
 					counter++;
 				}
 			}
 		}
 		System.out.println("Total lookups are = "+counter);
 		long time2 = System.currentTimeMillis();
		System.out.println("Total time = "+(time2-time1));
	}
	
	public void mongoDB_example(){
		// Code Snippet for MongoDB
				MongoCollection<Document> coll = mongodb.getCollection("A:B:C");
				
				for(int i=0;i<5;i++){
					for(int j = 0;j<5;j++){
//						BasicDBObject query = new BasicDBObject(i+":"+j, new BasicDBObject("$exists",true));
						BasicDBObject searchQuery = new BasicDBObject().append("key", i+":"+j);
						
//						Document result = coll.find(query).first();
//						System.out.println(result+"            "+result.get("_id"));
//						Document documents = (Document) result.get(i+":"+j);
//						System.out.println(documents);

						for(int k = 0;k<10;k++){
							if(coll.find(searchQuery).first()!=null){
								Document result = coll.find(searchQuery).first();
//								Document documents = (Document) result.get("value");
								List<Long> documents = (List<Long>) result.get("value");
								documents.add((long) k);
								coll.replaceOne(searchQuery, result);
								
//								if(documents.containsKey(""+k)==false){
//									documents.put(""+k, 1);
//									coll.replaceOne(searchQuery, result);
//								}
							}else{
								System.out.println(i+":"+j);
								List<Long> tempList = new ArrayList<Long>();
								tempList.add((long) k);
								coll.insertOne(new Document("value", tempList).append("key", i+":"+j));
//								coll.replaceOne(query, result);
							}					
						}
//						System.out.println(documents);
//						System.out.println(result);
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
				mongodb.dropDatabase();
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date));
		
		Colocation colocation_instance = new Colocation();
		
//		Delete all database which contain ":" in their name
		List<String> dbNames = mongoClient.getDatabaseNames();
		for(int i = 0;i < dbNames.size();i++){
			String dbName = dbNames.get(i);
			if(dbName.contains(":")==true || dbName.equals("spatium")){
				System.out.println("Database dropped = "+dbName);
				mongoClient.dropDatabase(dbName);
			}			
		}
//		verify_areConnected();
		
		HashMap<String, HashMap<String, Float>> Lk;
		HashSet<List<String>> Ck = new HashSet<List<String>>();
		Ck = L1();
		print_Candidate(Ck, 2);
		
		for(int k = 2;;k++){
			date = new Date();
			System.out.println(dateFormat.format(date));
			
			if(k==2){
//				Lk = L2();
				Lk = multithreaded_L2(Ck, 2, false);
			}else if (k==3) {
				Lk = multithreaded_L3(Ck, k, false);
//				L3(Ck, k, false);
			}else {
				Lk = multithreaded_L3(Ck, k, false);
//				Lk = Lk(Ck, k, false);
			}
			print_Frequent(Lk, k);
			Ck = join_and_prune(Lk, k);
			print_Candidate(Ck, k+1);
			
			if(Ck.isEmpty()){
				break;
			}
		}		
		
		db.closeTitanGraph(graph);
		
	}
}
