import java.util.ArrayList;
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

public class Baseline {
	
	public static Database db;
	public static TitanGraph graph;
	public static HashMap<String, Long> total_count; 
	public static double PI_threshold;
	public static boolean verbose;
	public static ConcurrentHashMap<List<String>,Float>  colocations;
	
	public Baseline(){
		this.db = new Database();
		this.graph = this.db.connect();
		this.total_count = new HashMap<String, Long>();
		this.PI_threshold = 0.1;
		this.verbose = false;
		this.colocations = new ConcurrentHashMap<List<String>,Float>();
	}
	
	public Baseline(Database db, TitanGraph graph){
		this.db = db;
		this.graph = graph;
		this.total_count = new HashMap<String, Long>();
		this.PI_threshold = 0.001;
		this.verbose = false;
		this.colocations = new ConcurrentHashMap<List<String>,Float>();
	}

	public static HashSet<List<String>> L1(){
		/*
		 * Generate colocations of size 1
		 * Iterate over all vertices of the graph
		 */
		System.out.println("Generating colocations of size 1\n");
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
//				System.out.println(type1+":"+type2);
				counter++;
			}
		}
		System.out.println();
		System.out.println("Total frequent colocations of size "+k+" are "+counter);
	}
	
	public static HashMap<String, HashMap<String, Float>> L2(HashSet<List<String>> Ck){
		/*
		 * Generate colocations of size 2
		 * Iterate over all edges of the graph
		 */
		
		HashMap<String, HashMap<Long, Boolean>> global_count = new HashMap<String, HashMap<Long, Boolean>>();
		HashMap<String, HashMap<String, Float>> L2 = new HashMap<String, HashMap<String,Float>>();
		
		long time1 = System.currentTimeMillis();
		
		if(verbose){
			System.out.println("Generating colocations of size 2.\n");
		
		}
		int counter = 0;
		//could lead to buffer-overflow
		
//		GremlinPipeline pipe = new GremlinPipeline();
		
		Iterator it;
		it = Ck.iterator();
		String candidate, type1, type2;
		while(it.hasNext()){
			List<String> tempList = ((List<String>)it.next());
			type1 = tempList.get(0);
			type2 = tempList.get(1);
			candidate = type1+"-"+type2;
			
//			System.out.println("Candidate being screwed right now is : "+candidate);
			
			final List<Vertex> temp = new ArrayList<Vertex>();
			GremlinPipeline pipe = new GremlinPipeline();
			pipe.start(graph.getVertices("type",type1)).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(candidate).out(candidate).filter(new PipeFunction<Vertex,Boolean>() {
				  public Boolean compute(Vertex argument) {
					  if(temp.contains(argument)){
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
			
			if(global_count.containsKey(type1+"-"+type2)==false){
				HashMap<Long, Boolean> default_hashmap = new HashMap<Long, Boolean>();
				global_count.put(type1+"-"+type2, default_hashmap);
			}
			if(global_count.containsKey(type2+"-"+type1)==false){
				HashMap<Long, Boolean> default_hashmap = new HashMap<Long, Boolean>();
				global_count.put(type2+"-"+type1, default_hashmap);
			}
			
				
			Iterator pit = pipe.iterator();
			
			while(pit.hasNext()){
				
//				System.out.println(pit.next().get(0).getClass());
				List<Long> tempList1 = ((List<Long>)pit.next());
				Long Id1 = tempList1.get(0);
				Long Id2 = tempList1.get(1);
//				//can be made cleaner if need be...but its java in the end so how much cleaner could it be :P
				if(global_count.get(type1+"-"+type2).containsKey(Id1) == false ){
					global_count.get(type1+"-"+type2).put(Id1, true);
				}
				if(global_count.get(type2+"-"+type1).containsKey(Id2) == false ){
					global_count.get(type2+"-"+type1).put(Id2, true);
				}
			}
			
			Double x1 = (double) global_count.get(type1+"-"+type2).size();
			Double x2 = (double) global_count.get(type2+"-"+type1).size();
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
		
		
		
		if (verbose){
			System.out.println("Total no. of colocations of size 2  are = "+counter);
		}
		long time2 = System.currentTimeMillis();
		System.out.println("Time taken for size 2 : "+(time2-time1));
		return L2;
		
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
	
	public static void main(String[] args) {
		
		Baseline baseline = new Baseline();
		
//		HashMap<String, HashMap<String, Float>> Lk;
		HashSet<List<String>> C2 = new HashSet<List<String>>();
		HashMap<String, HashMap<String, Float>> C3 = new HashMap<String, HashMap<String, Float>>();
		C2 = L1();
//		print_Candidate(Ck, 2);
		C3 = L2(C2); 
		print_Frequent(C3, 2);
		
		db.close(graph);
		
	}

}
