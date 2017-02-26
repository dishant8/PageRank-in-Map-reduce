package matrix;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReducerForParser extends Reducer<Text,InlinkAndContribution,Text, Text> {	
	int counter;
	MultipleOutputs<Text, Text> ms;
	Map<String, Integer> identifier;
	List<String> danglingList;
	
	
	// Setting up MultipleOutputs object
	@Override
	protected void setup(Reducer<Text, InlinkAndContribution, Text, Text>.Context context) throws IOException, InterruptedException {
		ms = new MultipleOutputs<Text, Text>(context);
		identifier = new HashMap<String, Integer>();
		danglingList = new ArrayList<String>();
		counter = 0;
	}
	
	
	// In clean up method dangling nodes, page rank vector and tracking of pages 
	// are written to a file using multiple output
	@Override
	protected void cleanup(Reducer<Text, InlinkAndContribution, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		// writing it to a tracking file which keeps the track of 
		// page and its equivalent integer identifier
		for(Map.Entry<String, Integer> entry : identifier.entrySet()){
			ms.write("Tracking", new Text(entry.getKey()), new Text(entry.getValue().toString()), "tracking");
		}
		
		int vectorSize = identifier.size();
		double defaultPageRank = 1.0/vectorSize;
		
		// Writes the default page rank vector
		for(int i = 1; i<= vectorSize; i++){	
			ms.write("PageRankVector", new Text(Integer.toString(i)), 
					new Text(Double.toString(defaultPageRank)),  "pageRank0/part");			
		}
		
		// Writing a list of dangling nodes to a file.
		for(int i = 0; i< danglingList.size(); i++){
			ms.write("DanglingFile", new Text(danglingList.get(i).toString()), new Text(""), "dangling");
		}
		
		ms.close();
	}
	
	public void reduce(Text key, Iterable<InlinkAndContribution> values, Context context) throws IOException, InterruptedException {
    	StringBuilder allInlinks = new StringBuilder();
    	    	
    	int pageIdentifier = 0;    	
    		// Check if page is present in hashMap, if yes get its equivalent identifier
    		// if no, add new entry to hash map after incrementing the counter. 
        	if(identifier.get(key.toString()) != null){
        		pageIdentifier = identifier.get(key.toString());
        	}
        	else{
            	counter++;
            	pageIdentifier = counter;
            	identifier.put(key.toString(), counter);	
        	}
    	
        	boolean checkIfDangling = true;
        	// Iterating over values and creating the sparse matrix with page 
        	// and all its in-links with their contribution
        	
        	for(InlinkAndContribution val: values){        		
        		if(!val.getInLink().equals("")){        			
            		if(identifier.get(val.getInLink()) != null){
            			allInlinks.append(identifier.get(val.getInLink()));
            			allInlinks.append(":");
            			allInlinks.append(val.getContribution());
            			allInlinks.append(",");
            		}else{
            			counter++;
            			identifier.put(val.getInLink(), counter);
            			allInlinks.append(counter);
            			allInlinks.append(":");
            			allInlinks.append(val.getContribution());
            			allInlinks.append(",");    			
            		}
        		}
        		
        		if(val.getSizeOfAdjacency() > 0){
        			checkIfDangling = false;
        		}    		
    		}
    	
        	// add dangling nodes to the dangling list as size of adjacency is 0.
        	if(checkIfDangling){
    			danglingList.add(identifier.get(key.toString()).toString());
        	}

	    	String finalString = "";
	    
	    	if(allInlinks != null && allInlinks.length() > 0){
	        	finalString = allInlinks.substring(0, allInlinks.length()-1);    		
	    	}
	    	// emitting the page and all its in-links and contribution concatenated
	    	ms.write("OutputFile", new Text(Integer.toString(pageIdentifier)), new Text(finalString.toString()), "contributions" );
    }
}