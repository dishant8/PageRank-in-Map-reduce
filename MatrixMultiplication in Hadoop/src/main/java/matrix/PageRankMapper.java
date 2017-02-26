package matrix;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Map call for page rank mapper reads the sparse matrix generated in the last job, iterates over 
// each of its inlink, looks up over the page rank vector, get the page rank value from last iteration,
// performs the multiplication between the contribution of inlink and its pagerank value and at the end of 
// the map call gets the summation of the contribution of all of its inlink and calculates new page rank value.
 
public class PageRankMapper extends Mapper<Object , Text, Text, Text>{
	// Set to store dangling nodes from the distributed cache
	Set<String> danglingNodes;
	
	// Map to store the page rank vector
	Map<Integer,String> pageRankMap;
	
	// Tracks the mapping of page and its identifier 
	Map<Integer,String> tracker;
	double totalDanglingValue = 0.0;
	
	
	// Setup method reads the file from distributed cache generated in last job 
	// and populates the corresponding data structures. Distributed cache helps to cache the 
	// same page rank vector across multiple map calls.
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		pageRankMap = new HashMap<Integer, String>();
		tracker = new HashMap<Integer, String>();
		danglingNodes = new HashSet<String>();
	
		// Reading the dangling cache file and populating the data structure
		BufferedReader bf= new BufferedReader(new FileReader("./dangling-r-00000"));
		String line;		
		while((line = bf.readLine()) != null){
				danglingNodes.add(line);
		}
		
		// Iterating over multiple pageRank files generated in reducer and adding it to Hash map.
		File directory = new File("./pageRank" + context.getConfiguration().getInt("iterations", -10));
		FileFilter fileFilter = new WildcardFileFilter("part*");
		
		File[] fileArray = directory.listFiles(fileFilter);
		for(File eachFile: fileArray){
			pageRankFile(eachFile.getPath());
		}
		
		// Iterating over danglingNodes and looking up over page rank vector to calculate 
		// the dangling node contribution to calculate page rank value.
		Iterator<String> iterator = danglingNodes.iterator();
		
		while(iterator.hasNext()){
			int eachDangling = Integer.parseInt(iterator.next().trim());
			double pageRankValue = Double.parseDouble(pageRankMap.get(eachDangling));
			totalDanglingValue += pageRankValue;
		}
	}

	private void pageRankFile(String fileName) throws IOException {
		BufferedReader bf1 = new BufferedReader(new FileReader(fileName));		
		String line1;
		while((line1 = bf1.readLine()) != null){
			String[] pageRank = line1.split("\t");
			pageRankMap.put(Integer.parseInt(pageRank[0]), pageRank[1]);
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    						
		// Gets the total number of pages involved in this job
		long numberOfPages = context.getConfiguration().getLong("numberOfPages", -10);
		
		// String manipulations to get the page with its inlinks and conribution from the text file
		String[] line = value.toString().split("\t");
		String nid = line[0];

		if(line.length > 1){
			// All the inlinks for a page and their contribution 
			String[] inLinks = line[1].toString().split(",");			
			
			double totalValue = 0.0;			
			
			// Iterating over the links and summing up its contribution by performing multiplication
			// by looking up over the page rank vector for that particular inlink
			for(int i = 0; i < inLinks.length; i++){			
				String[] elementAndContribution = inLinks[i].split(":");
				String pageRankValue = pageRankMap.get(Integer.parseInt(elementAndContribution[0]));			
				double sum = Double.parseDouble(pageRankValue) * Double.parseDouble(elementAndContribution[1]);				
				totalValue += sum;
			}
			
			// calculating new page rank value
			double newPageRankValue = (0.15/pageRankMap.size()) + 
					(0.85 * (totalValue + (totalDanglingValue/pageRankMap.size())));   	
			
			context.write(new Text(nid.toString()), 
					new Text(String.valueOf(newPageRankValue)));			
		}else{
			double newPageRankValue = (0.15/pageRankMap.size()) + 
					(0.85 * (0 + (totalDanglingValue/pageRankMap.size()))) ;
			context.write(new Text((nid.toString())),
					new Text(String.valueOf(newPageRankValue)));			
		}
		
   }
}
