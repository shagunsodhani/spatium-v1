import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.Enum;

import javax.validation.constraints.Min;

import org.antlr.grammar.v3.ANTLRv3Parser.notSet_return;
import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.cli.CliParser.typeIdentifier_return;
import org.apache.lucene.util.fst.PairOutputs;

import sun.font.Type1Font;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Pair;

public class Colocation {
	
	public static Database db = new Database();
	public static TitanGraph graph = db.connect();
	public static HashMap<String, Long> total_count = new HashMap<String, Long>(); 
	public static double PI_threshold = 0.4;
	public static boolean verbose = false;
	
	public static void L1(){
		/*
		 * Generate colocations of size 1
		 * Iterate over all vertices of the graph
		 */

		System.out.println("Generating colocations of size 1.\n");
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
	}
	
	public static HashSet<List<String>> L2(){
		/*
		 * Generate colocations of size 2
		 * Iterate over all edges of the graph
		 */
		
		HashMap<String, HashMap<Long, Boolean>> global_count = new HashMap<String, HashMap<Long, Boolean>>();
		HashMap<String, HashSet<String>> temp_C3 = new HashMap<String, HashSet<String>>();
		
		System.out.println("Generating colocations of size 2.\n");
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
			}System.out.println(type1);
			System.out.println(type2);
			
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
			Double x1 = (double) global_count.get(type1+":"+type2).size();
			Double x2 = (double) global_count.get(type2+":"+type1).size();
			Double a = x1/total_count.get(type1);
			Double b = x2/total_count.get(type2);
			Double PI = java.lang.Math.min(a, b);
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
				System.out.println(type1+":"+type2);
				System.out.println(PI);
				System.out.println("\n");
				counter+=1;
				
				if(temp_C3.containsKey(type1)==false){
					HashSet<String> tempset = new HashSet<String>();
					tempset.add(type2);
					temp_C3.put(type1, tempset);
				}
				else{
					temp_C3.get(type1).add(type2);
				}
			}
		}
		
		if (verbose){
			System.out.println("Total no. of colocations of size 2  are = "+counter);
		}
		
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
				it3 = temp_C3.get(string2).iterator();
				while(it3.hasNext()){
					string3 = (String) it3.next();
					if(temp_C3.get(string1).contains(string3))
					{
						System.out.println("\n");
						System.out.println(string1);
						System.out.println(string2);
						System.out.println(string3);
						System.out.println("\n");
						List<String> temp_list = new ArrayList();
						temp_list.add(string1);
						temp_list.add(string2);
						temp_list.add(string3);
						C3.add(temp_list);
					}
				}
			}
		}
		return C3;	
	}
	
	public static void Lk(){
		/*
		 * Generate colocation of size k
		 */
	}
	public static void main(String[] args) {
		L1();
		L2();
	}
}
