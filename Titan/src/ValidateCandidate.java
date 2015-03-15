import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;


public class ValidateCandidate extends Thread{
	
	private List<String> tempList;
	private MongoDatabase mongodb;
	private TitanGraph graph;
	private HashMap<String, Long> total_count;
	private double PI_threshold;
	private boolean verbose;
	private int k;

	public ValidateCandidate(List<String> candidate, int k) {
		this.tempList = candidate;
		this.mongodb = Colocation.mongodb;
		this.graph = Colocation.graph;
		this.total_count = Colocation.total_count;
		this.PI_threshold = Colocation.PI_threshold;
		this.verbose = Colocation.verbose;
		this.k = k;
	}
	
	public void run() {
		
		int total_cliques = 0;
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
		String databaseName = "";
		
		for(int i=0; i < tempList.size(); i++){
			String key = tempList.get(i);
			
			if(unique.containsKey(key)==false){
				HashSet<Long> tempHashSet = new HashSet<Long>();
				unique.put(key, tempHashSet);
			}
			databaseName += key+":";
		}
		databaseName = databaseName.substring(0, databaseName.length()-1);
		mongoDB mongoInstance = new mongoDB(databaseName);
		MongoDatabase mongoDatabase = mongoInstance.connect();
				
		String type1 = "";
		int i, j;
		for(i = 0; i < k-2; i++){
			type1 += tempList.get(i)+":";
		}
		type1 = type1.substring(0, type1.length()-1);
		String type2 = tempList.get(k-2);
		String type3 = tempList.get(k-1);

		// Initialize the collection A:B:C
		MongoCollection<Document> coll =  mongoDatabase.getCollection(type1+":"+type2+":"+type3);
				
		Iterator<Vertex> it1 = graph.query().has("type", Compare.EQUAL, type1).vertices().iterator();
		while(it1.hasNext()){
			
			Vertex vertex1 = it1.next();
			long id1 = (long) vertex1.getId();
			
			Iterator<Vertex> it2 = vertex1.getVertices(Direction.OUT, type1+"-"+type2).iterator();
							
			while (it2.hasNext()) {
				Vertex vertex2 = (Vertex) it2.next();
				long id2 = (long) vertex2.getId();
			
				Iterator<Vertex> it3 = vertex1.getVertices(Direction.OUT, type1+"-"+type3).iterator();
				while (it3.hasNext()) {
					Vertex vertex3 = (Vertex) it3.next();
					long id3 = (long) vertex3.getId();
					
					if(Colocation.areConnected(id2, id3)==true){
						
						if(unique.get(type1).contains(id1)==false){
							unique.get(type1).add(id1);
						}
						if(unique.get(type2).contains(id2)==false){
							unique.get(type2).add(id2);
						}
						if(unique.get(type3).contains(id3)==false){
							unique.get(type3).add(id3);
						}
						
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
		if(pi >= PI_threshold){
			System.out.println(type1+":"+type2+":"+type3+" => "+pi);
			Colocation.colocations.put(tempList, (double) pi);
			
			mongoDatabase.dropDatabase();
		}else{
			mongoDatabase.dropDatabase();
		}
		
	}
	

}
