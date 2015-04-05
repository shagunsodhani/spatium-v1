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
import com.sun.org.apache.bcel.internal.generic.L2D;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;

public class ValidateTraversal extends Thread{
	private List<String> tempList;
	private TitanGraph graph;
	private HashMap<String, Long> total_count;
	private double PI_threshold;
	private boolean verbose;
	private int k;
	
	public ValidateTraversal(List<String> candidate, int k) {
		this.tempList = candidate;
		this.graph = Baseline.graph;
		this.total_count = Baseline.total_count;
		this.PI_threshold = Baseline.PI_threshold;
		this.verbose = Baseline.verbose;
		this.k = k;
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
				Baseline.colocations.put(tempList, PI);
			}					
		}
	}
	
	public void L3(){
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
		String type1,type2,type3;
		type1 = tempList.get(0);
		type2 = tempList.get(1);
		type3 = tempList.get(2);
		for(int i=0;i<tempList.size();i++)
		{
			String key = tempList.get(i);
			if(unique.containsKey(key)==false)
			{
				HashSet<Long> tempHashSet = new HashSet<Long>();
				unique.put(key, tempHashSet);
			}
		}
		String candidate = type1+":"+type2+":"+type3;
		
//		System.out.println("Candidate being screwed right now is : "+candidate);
		
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
			}).in(type1+"-"+type2).in(type2+"-"+type3).out(type1+"-"+type3).filter(new PipeFunction<Vertex,Boolean>() {
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
					
		Iterator pit = pipe.iterator();
		
		while(pit.hasNext())
		{
//			System.out.println(pit.next().get(0).getClass());
			List<Long> tempList1 = ((List<Long>)pit.next());
			Long Id1 = tempList1.get(0);
			Long Id2 = tempList1.get(1);
			Long Id3 = tempList1.get(2);
			unique.get(type1).add(Id1);
			unique.get(type2).add(Id2);
			unique.get(type3).add(Id3);
		}
		
		float count_type1 = total_count.get(type1);
		float count_type2 = total_count.get(type2);
		float count_type3 = total_count.get(type3);
		float pr_type1 = ((float) unique.get(type1).size())/count_type1;
		float pr_type2 = ((float) unique.get(type2).size())/count_type2;
		float pr_type3 = ((float) unique.get(type3).size())/count_type3;
		float pi = pr_type1;
		if(pr_type2 < pi)
		{
			if(pr_type3 < pi)
			{
				pi = pr_type3;
			}
			else
			{
				pi = pr_type2;
			}
		}				
		else
		{
			if(pr_type3 < pi)
			{
				pi = pr_type3;
			}
		}
		if(pi>=PI_threshold)
		{
			if(verbose)
			{
				System.out.println("--------------");
				System.out.println("Frequent = "+type1+":"+type2+":"+type3+" PI = "+pi);
				System.out.println("Unique_Count = "+unique.get(type1).size()+":"+unique.get(type2).size()+":"+unique.get(type3).size());
				System.out.println("Total_Count = "+count_type1+":"+count_type2+":"+count_type3);
			}
			Baseline.colocations.put(tempList, pi);
		}	
	}
	
	public void Lk(){
		
		ArrayList<String> type = new ArrayList<String>();
		HashMap<String, HashSet<Long>> unique = new HashMap<String, HashSet<Long>>();
		for(int i=0;i<k;i++)
		{
			String key = tempList.get(i);
			if(unique.containsKey(key)==false)
			{
				HashSet<Long> tempHashSet = new HashSet<Long>();
				unique.put(key, tempHashSet);
			}
			type.add(key);
		}
		
		final List<Vertex> temp = new ArrayList<Vertex>();
		GremlinPipeline pipe = new GremlinPipeline();
		
		if (k==4)
		{
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).out(type.get(0)+"-"+type.get(3)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}
		else if (k==5) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).out(type.get(0)+"-"+type.get(4)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}		
		else if (k==6) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).in(type.get(4)+"-"+type.get(5)).out(type.get(0)+"-"+type.get(5)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}		
		else if (k==7) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).in(type.get(4)+"-"+type.get(5)).in(type.get(5)+"-"+type.get(6)).out(type.get(0)+"-"+type.get(6)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}		
		else if (k==8) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).in(type.get(4)+"-"+type.get(5)).in(type.get(5)+"-"+type.get(6)).in(type.get(6)+"-"+type.get(7)).out(type.get(0)+"-"+type.get(7)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}
		else if (k==9) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).in(type.get(4)+"-"+type.get(5)).in(type.get(5)+"-"+type.get(6)).in(type.get(6)+"-"+type.get(7)).in(type.get(7)+"-"+type.get(8)).out(type.get(0)+"-"+type.get(8)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}
		else if (k==10) {
			pipe.start(graph.getVertices("type",type.get(0))).sideEffect(new PipeFunction<Vertex, Vertex>(){
				public Vertex compute(Vertex argument){
					if(temp.size()>0){
						temp.remove(temp.size()-1);
					}
					temp.add(argument);
					return argument;
				}
				}).in(type.get(0)+"-"+type.get(1)).in(type.get(1)+"-"+type.get(2)).in(type.get(2)+"-"+type.get(3)).in(type.get(3)+"-"+type.get(4)).in(type.get(4)+"-"+type.get(5)).in(type.get(5)+"-"+type.get(6)).in(type.get(6)+"-"+type.get(7)).in(type.get(7)+"-"+type.get(8)).in(type.get(8)+"-"+type.get(9)).out(type.get(0)+"-"+type.get(9)).filter(new PipeFunction<Vertex,Boolean>() {
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
		}
			
		Iterator pit = pipe.iterator();
		
		while(pit.hasNext())
		{
//			System.out.println(pit.next().get(0).getClass());
			List<Long> IdList = ((List<Long>)pit.next());
			Boolean flag = true;
			outerloop:
			for (int i = 0; i < k-2; i++) 
			{
				for (int j = i+2; j < k; j++) 
				{
					if(Baseline.areConnected(IdList.get(i), IdList.get(j),type.get(i)+"-"+type.get(j)) == false)
					{
						flag = false;
						break outerloop;
					}
				}
			}
			if(flag==false)
				continue;
			for (int i = 0; i < k; i++) 
			{
				unique.get(type.get(i)).add(IdList.get(i));
			}
		}
		
		float ParticipationIndex = (float)1.0;
		for (int x = 0; x < k; x++) 
		{
			float ParticipationRatio = unique.get(type.get(x)).size()/((float)total_count.get(type.get(x))); 
			if(ParticipationIndex > ParticipationRatio)
			{
				ParticipationIndex = ParticipationRatio;
			}
		}
		if(ParticipationIndex >= PI_threshold)
		{
			Baseline.colocations.put(tempList, ParticipationIndex);			
		}
	}
}
