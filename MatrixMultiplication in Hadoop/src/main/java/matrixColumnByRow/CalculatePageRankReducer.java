package matrixColumnByRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculatePageRankReducer extends Reducer<Text,Text,Text, Text> {
		

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	long danglingValue = context.getConfiguration().getLong("danglingNode", -10);		
    	long numberOfNodes = context.getConfiguration().getLong("numberOfNodes", -10);
		// For iteration one just emits the pagName and the Node which is the adjacency list and pageRank value. 
		// Thus after the first iteration it has default page rank value for each page and same graph as parser's output.
//		System.out.println("DANGLING VALUE ++++++ \n\n\n\n\n\n\n" + danglingValue);
		double sumOfContribution = 0.0; 
		for(Text val : values){
			sumOfContribution += Double.parseDouble(val.toString());			
		}
		
		double newPageRankValue = (0.15/numberOfNodes) + 
				(0.85 * (sumOfContribution + ((danglingValue/Math.pow(10, 9))/numberOfNodes)));   	
		
		context.write(key, new Text(Double.toString(newPageRankValue)));

    }
}
