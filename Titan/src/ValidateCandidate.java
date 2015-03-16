import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class ValidateCandidate extends Thread{
	
	private List<String> tempList;
	private MongoDatabase mongodb;
	private TitanGraph graph;
	private HashMap<String, Long> total_count;
	private double PI_threshold;
	private boolean verbose;
	private int k;
	private boolean create_db;
	
	public ValidateCandidate(List<String> candidate, int k, boolean created_db) {
		this.tempList = candidate;
		this.mongodb = Colocation.mongodb;
		this.graph = Colocation.graph;
		this.total_count = Colocation.total_count;
		this.PI_threshold = Colocation.PI_threshold;
		this.verbose = Colocation.verbose;
		this.k = k;
		this.create_db = created_db;
	}
	
	public void run() {
		
		int total_cliques = 0;
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
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
		
		String dbname1 = type1+":"+type2+":"+type3;
		MongoCollection<Document> coll ;						
		if (create_db==true){
			mongoDB new_mongoInstance = new mongoDB(dbname1);
			MongoDatabase new_mongodb = new_mongoInstance.connect();
			coll = new_mongodb.getCollection(dbname1);
		}
		else{
			coll = mongodb.getCollection(dbname1);
		}
				
		Iterator<Vertex> it1 = graph.query().has("type", Compare.EQUAL, type1).vertices().iterator();
		while(it1.hasNext()){
			Vertex vertex1 = it1.next();
			long id1 = (long) vertex1.getId();
			List<Long> v2 = new ArrayList<Long>();
			List<Long> v3 = new ArrayList<Long>();
			boolean flag_outer = false;
			Iterator<Vertex> it2 = vertex1.getVertices(Direction.IN, type1+"-"+type2).iterator();
			while(it2.hasNext()){
				Vertex vertex2 = it2.next();
				v2.add((long) vertex2.getProperty("id"));
			}
			Iterator<Vertex> it3 = vertex1.getVertices(Direction.IN, type1+"-"+type3).iterator();
			while(it3.hasNext()){
				Vertex vertex3 = it3.next();
				v3.add((long) vertex3.getProperty("id"));
			}
			for(int i =0 ; i < v2.size();i++){
				boolean flag_inner = false;
				List<Long> temp_list = new ArrayList<Long>();

				for(int j =0; j < v3.size(); j++){
						if(Colocation.areConnected(v2.get(i), v3.get(j))==true){
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
				Colocation.mongoClient.dropDatabase(dbname1);
//				mongoClient.dropDatabase(type1+":"+type2+":"+type3);
			}
		}
	}
}


