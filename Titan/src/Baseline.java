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
		HashSet<List<String>> Ck = new HashSet<List<String>>();
		Ck = L1();
		print_Candidate(Ck, 2);
		
		db.close(graph);
		
	}

}
