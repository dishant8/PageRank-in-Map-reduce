package hw3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Map call for page rank mapper reads the output from last iteration, and emits each element of the 
// adjacency list with its contribution i.e PageRankValue/ Size of AdjacencyList, also calculates the sum
// of the page rank of the dangling nodes which will account in the reducer of the next Job
 
public class PageRankMapper extends Mapper<Object , Text, Text, Node>{
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    		
		// Gets the total number of pages involved in this job
		long numberOfPages = context.getConfiguration().getLong("numberOfPages", -10);
		
		// String manipulations to get the page rank value and adjacency list from the text file
		String[] line = value.toString().split("\t");    		
		String nid = line[0];
		String[] valueAndList = line[1].split("~");
		int iteration = context.getConfiguration().getInt("itr", -10);
		
		// First iteration for page rank calculates only the sum of the page rank for the dangling node, so that actual 
		// iteration accounts for the dangling nodes when calculating the new page rank in the reducer. The mapper emits 
		// the same graph with default page rank value  - 1/N for all pages.
		
		if(iteration == 0){
			// Check whether page has adjacency list, or it is a dangling node
			if(valueAndList.length > 1){
				String[] adjacencyMembers = valueAndList[1].split(",");
				List<String> listForAdjacentMembers = new ArrayList<String>();
				// Create an adjacency list for a particular page
				for(String eachMembers: adjacencyMembers){
					listForAdjacentMembers.add(eachMembers);
				}
				context.write(new Text(nid), new Node((double)1/(numberOfPages), listForAdjacentMembers, ""));
				
			}else{
				// Increment the global counter for the dangling node with the default page rank value 
    			context.getCounter(Driver.COUNTER.DANGLINGNODE).increment((long)((1.0/numberOfPages) * Math.pow(10, 9)));				
				context.write(new Text(nid), new Node((double) 1/(numberOfPages), new ArrayList<String>(), ""));
			}
						
		}
		else{
			// Check whether page has adjacency list, or it is a dangling node
			if(valueAndList.length > 1){
				String[] adjacencyMembers = valueAndList[1].split(",");
				List<String> listForAdjacentMembers = new ArrayList<String>();
				// Create an adjacency list for a particular page
				for(String eachMembers: adjacencyMembers){
					listForAdjacentMembers.add(eachMembers);
				}
				// Each page divides it page rank value among the members in its adjacency 
				// list and emits adjacent member and this value
				double contributionToAdjacentMember = Double.parseDouble(valueAndList[0])/adjacencyMembers.length;
				for(String str : adjacencyMembers){					
    				context.write(new Text(str), new Node(contributionToAdjacentMember, new ArrayList<String>(), ""));
				}
				// Emits the entire graph for this particular page i.e its adjacency list and page rank value 
				// as well as pageName to differentiate in a reducer if it is a graph or contributions to that page 
				context.write(new Text(nid), new Node(Double.parseDouble(valueAndList[0]), listForAdjacentMembers, nid));
			}
			else{
				// Increment the global counter for the dangling node with the page rank value of the page 
    			context.getCounter(Driver.COUNTER.DANGLINGNODE).increment((long)(Double.parseDouble(valueAndList[0]) * Math.pow(10, 9)));
    			// Emit the dangling node
        		context.write(new Text(nid), new Node(0.0, new ArrayList<String>(), "")); 			
			}
	}
   }
}
