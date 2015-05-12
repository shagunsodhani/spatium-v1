import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.sun.org.apache.bcel.internal.generic.L2D;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;

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
		if(k>3){
			Lk();
		}
		else if(k==3){
			L3();
		}else{
			L2();
		}
	}

	public void L2(){
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
		for(int i=0;i<tempList.size();i++){
			String key = tempList.get(i);
			if(unique.containsKey(key)==false){
				HashSet<Long> tempHashSet = new HashSet<Long>();
				unique.put(key, tempHashSet);
			}
		}
		
		String type1, type2;
//		List<String> tempList = ((List<String>)it.next());
		type1 = tempList.get(0);
		type2 = tempList.get(1);
		String candidate = type1+":"+type2;
		
//		System.out.println("Candidate being screwed right now is : "+candidate);
		
		GremlinPipeline pipe = new GremlinPipeline();
		
		pipe.start(graph.getVertices("type",type1)).in(type1+"-"+type2).path(new PipeFunction<Vertex, Long>(){
			public Long compute(Vertex argument) {
				return (Long) argument.getId();
			}
		}).enablePath();
				
		Iterator pit = pipe.iterator();
		
		while(pit.hasNext()){
			
//			System.out.println(pit.next().get(0).getClass());
			List<Long> tempList1 = ((List<Long>)pit.next());
			Long Id1 = tempList1.get(0);
			Long Id2 = tempList1.get(1);
			
			unique.get(type1).add(Id1);
			unique.get(type2).add(Id2);
		}
		
		Double x1 = (double) unique.get(type1).size();
		Double x2 = (double) unique.get(type2).size();
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
			if(PI >= PI_threshold){
				Colocation.colocations.put(tempList, PI);
			}					
		}
	}
	
	public void L2_old(){
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
		
		for (Iterator<Edge> iterator = graph.getEdges("label", type1+"-"+type2).iterator(); iterator.hasNext();) {
			Edge edge = iterator.next();
			if (verbose){
//				System.out.println(counter+" : "+" Edge Label = "+edge.getLabel()+" Distance = "+edge.getProperty("distance"));
			}
			Vertex vertex1 = edge.getVertex(Direction.IN);
//			String type1 = vertex1.getProperty("type");
			Long Id1 = (Long) vertex1.getId();
			Vertex vertex2 = edge.getVertex(Direction.OUT);
//			String type2 = vertex2.getProperty("type");
			Long Id2 = (Long) vertex2.getId();
			//could lead to buffer-overflow	
						
			unique.get(type1).add(Id1);
			unique.get(type2).add(Id2);
		}
		
		Double x1 = (double) unique.get(type1).size();
		Double x2 = (double) unique.get(type2).size();
		Double a = x1/total_count.get(type1);
		Double b = x2/total_count.get(type2);
		float PI = (float) java.lang.Math.min(a, b);
		if(PI >= PI_threshold){
			Colocation.colocations.put(tempList, PI);
		}
	}
	
	public void L3(){
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
//		MongoDatabase new_mongodb;
		if (create_db==true){
			mongoDB new_mongoInstance = new mongoDB(dbname1);
			MongoDatabase new_mongodb = new_mongoInstance.connect(false);
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
//						if(Colocation.areConnected(v2.get(i), v3.get(j))==true){
						if(Colocation.areConnected(v2.get(i), v3.get(j),type2+"-"+type3)==true){
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
			CreateIndexOptions options = new CreateIndexOptions();
			options.background(true);
			coll.createIndex(new BasicDBObject("key",1),options);
			Colocation.colocations.put(tempList, pi);
		}
		else{
			if(create_db==true){
//				new_mongodb.dropDatabase();
				Colocation.mongoClient.dropDatabase(dbname1);
//				mongoClient.dropDatabase(type1+":"+type2+":"+type3);
			}
			else{
				coll.dropCollection();
			}
		}
	}
	
	public void Lk() {
		
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
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
		
		MongoCollection<Document> coll ;
		MongoCollection<Document> coll_1 ;
		MongoCollection<Document> coll_2 ;
		
		String dbname1, dbname2, dbname3;
		dbname1 = type1+":"+type2+":"+type3;
		dbname2 = type1+":"+type2;
		dbname3 = type1+":"+type3;
//		System.out.println(dbname1+"----"+dbname2+"-----"+dbname3);
		
		if (create_db==true){
			mongoDB new_mongoInstance = new mongoDB(dbname1);
			mongoDB new_mongoInstance_1 = new mongoDB(dbname2);
			mongoDB new_mongoInstance_2 = new mongoDB(dbname3);
			
			MongoDatabase new_mongodb = new_mongoInstance.connect(false);
			MongoDatabase new_mongodb_1 = new_mongoInstance_1.connect(false);
			MongoDatabase new_mongodb_2 = new_mongoInstance_2.connect(false);
			
			coll = new_mongodb.getCollection(dbname1);
			coll_1 = new_mongodb_1.getCollection(dbname2);
			coll_2 = new_mongodb_2.getCollection(dbname3);
			
		}
		else{
			coll = mongodb.getCollection(dbname1);
			coll_1 = mongodb.getCollection(dbname2);
			coll_2 = mongodb.getCollection(dbname3);
		}
		MongoCursor<Document> cursor_1 = (coll_1.find()).noCursorTimeout(true).iterator();
		try {
			
//			cursor_1.setOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			while (cursor_1.hasNext()) {
					Document doc_1 = cursor_1.next();
					String id1ist = doc_1.getString("key");
					BasicDBObject searchQuery = new BasicDBObject().append("key", id1ist);
					
					if(coll_2.find(searchQuery).first()!=null){
						
						Document doc_2 = coll_2.find(searchQuery).first();
						List<Long> doc_1_value =  (ArrayList<Long>) doc_1.get("value");
						List<Long> doc_2_value =  (ArrayList<Long>) doc_2.get("value");
						boolean flag_outer = false;
						for (i = 0; i < doc_1_value.size(); i++) {
							boolean flag_inner = false;
							List<Long> temp_list = new ArrayList<Long>();
							for (j=0; j<doc_2_value.size(); j++){
//								if(Colocation.areConnected(doc_1_value.get(i), doc_2_value.get(j))){
								if(Colocation.areConnected(doc_1_value.get(i), doc_2_value.get(j),type2+"-"+type3)){
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
			}
		} catch (MongoCursorNotFoundException exception) {
			// TODO: handle exception
			System.out.println("Cursor Not Found Exception "+dbname2+" and other colocations are "+dbname1+" and "+dbname3);
		}
		finally{
			cursor_1.close();
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
					Colocation.mongoClient.dropDatabase(dbname1);
				}
				else{
					coll.dropCollection();
				}
			}
			else{
				CreateIndexOptions options = new CreateIndexOptions();
				options.background(true);
				coll.createIndex(new BasicDBObject("key",1),options);
				Colocation.colocations.put(tempList, ParticipationIndex);
			}		
		}		
					
	}
}