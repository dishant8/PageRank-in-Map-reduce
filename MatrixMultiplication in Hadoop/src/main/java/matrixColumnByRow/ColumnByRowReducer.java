package matrixColumnByRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnByRowReducer extends Reducer<Text,Text,Text, Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> listForM = new ArrayList<String>();
		double pageRankValue = 0.0;
		// For each value check if it is M or R and perform the multiplication of 
		// contribution for each of adjacent element and its page rank value
		for(Text val : values){			
			String[] matrixTypeAndValue = val.toString().split(",");
			// check i matrix M
			if(matrixTypeAndValue[0].equals("M")){
				listForM.add(val.toString());
			}
			
			// Check if matrix R
			if(matrixTypeAndValue[0].equals("R")){
				pageRankValue = Double.parseDouble(matrixTypeAndValue[2]);
			}
		}
		
		// For each element in adjacency column calculate the contribution after multiplying 
		// with corresponding row.
		
		for(int i = 0; i < listForM.size(); i++){
			String[] matrixTypeAndValue = listForM.get(i).split(",");
			context.write(new Text(matrixTypeAndValue[1]), 
					new Text(Double.toString(Double.parseDouble(matrixTypeAndValue[2]) * pageRankValue)));			
		}
    }
}
