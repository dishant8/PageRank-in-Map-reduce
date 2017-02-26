package hw3;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageRankReducer extends Reducer<Text,Node,Text, Node> {
	
    public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
    	long danglingValue = context.getConfiguration().getLong("danglingNode", -10);
		long numberOfPages = context.getConfiguration().getLong("numberOfPages", -10);
		int iteration = context.getConfiguration().getInt("itr", -10);
		
		// For iteration one just emits the pagName and the Node which is the adjacency list and pageRank value. 
		// Thus after the first iteration it has default page rank value for each page and same graph as parser's output.
		
		if(iteration == 0){
			for(Node val : values){    		
				context.write(key, val);
			}			
		}
		else
		{
	    	double sum = 0.0;
	    	double alpha = 0.15;
	    	Node M = new Node(0.0, new ArrayList<String>(), "");
	    	int count = 0;

	    	for(Node val : values){    		
		    	// Emits the graph if the val is a graph and not the contribution
	    		if(val.getPageName().equals(key.toString())){
	    			M.setAdjacencylist(val.getAdjacencylist());
	    			count++;
	    		}else{
	    		// If not the graph it sums up the contributions of the inlinks of this page
	    			sum += val.getPageRank();
	    		}
	    		
	    	}
	    	Double pageRankValue = (alpha / numberOfPages) + ((1 - alpha) * (sum + (danglingValue/Math.pow(10, 9))/numberOfPages));
	    	M.setPageRank(pageRankValue);
	    	context.write(key, M);
		}

    }
}
