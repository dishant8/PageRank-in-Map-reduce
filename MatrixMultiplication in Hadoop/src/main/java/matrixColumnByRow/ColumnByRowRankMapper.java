package matrixColumnByRow;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Map call for page rank mapper reads the page rank vector from last iteration, and generates the R vector 
 
public class ColumnByRowRankMapper extends Mapper<Object , Text, Text, Text>{		
	Set<String> danglingNodes;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		danglingNodes = new HashSet<String>();
		// Use distributed cache to load the file which contains the dangling nodes
		BufferedReader bf= new BufferedReader(new FileReader("./dangling-r-00000"));
		String line;		
		while((line = bf.readLine()) != null){
				danglingNodes.add(line.trim());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    								
		// String manipulations to get the page rank value and adjacency list from the text file
		String[] pageAndValue = value.toString().split("\t");
		String nid = pageAndValue[0];
		
		// calculate the contributions of dangling nodes by adding up the page rank values for each dangling node
		
		if(danglingNodes.contains(nid.trim())){
			
			context.getCounter(Driver.COUNTER.DANGLINGNODE)
				.increment((long)(Double.parseDouble(pageAndValue[1]) * Math.pow(10, 9)));				
		}
		// Emitting the the page reference as key with identifier and page rank as value
		context.write(new Text(nid), new Text("R," + "-1," + new Text(pageAndValue[1])));
   }
}
