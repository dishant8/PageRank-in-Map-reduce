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


// Map call for page rank mapper reads the sparse matrix and emits each element in the
// adjacency list as key and an idenifier to represent it is an M matrix, page to which 
// it is an in-link along with its contribution as value 
 
public class ColumnByRowMapper extends Mapper<Object , Text, Text, Text>{		
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    						
			
		// String manipulations to get the page rank value and adjacency list from the text file
		String[] line = value.toString().split("\t");
		String nid = line[0];

		// check if number of in-links is not 0 
		if(line.length > 1){
			String[] inLinks = line[1].toString().split(",");
			
			for(int i = 0; i < inLinks.length; i++){				
				
				String[] elementAndContribution = inLinks[i].split(":");
				// emitting the inlink to the page as key, with identifier, page and contribution as value 
				context.write(new Text(elementAndContribution[0]), new Text("M," + nid + "," + elementAndContribution[1]));
			}
		}
		else{
			context.write(new Text((nid.toString())),
					new Text("M," + nid.toString() + ","+ "0"));			
		}
		
   }
}
