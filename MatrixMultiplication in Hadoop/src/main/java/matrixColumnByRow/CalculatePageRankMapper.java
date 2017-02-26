package matrixColumnByRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Map call for page rank mapper reads the output from last iteration, and emits each element of the 
// adjacency list with its contribution i.e PageRankValue/ Size of AdjacencyList, also calculates the sum
// of the page rank of the dangling nodes which will account in the reducer of the next Job
 
public class CalculatePageRankMapper extends Mapper<Object , Text, Text, Text>{		
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    						
		
		// String manipulations to get the page rank value and adjacency list from the text file
		String[] pageAndValue = value.toString().split("\t");
		
		context.write(new Text(pageAndValue[0]), new Text(pageAndValue[1]));
		
   }
}
