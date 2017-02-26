package hw3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// Only one reducer so it receives the top-100 local records from each mapper,
// Again using the tree set, it keeps the values sorted in ascending order 

public class TopKReducer extends Reducer<NullWritable,TopKCustomKey,NullWritable, TopKCustomKey> {
	
	private TreeSet<TopKCustomKey> recordSet = new TreeSet<TopKCustomKey>();
	
    public void reduce(NullWritable key, Iterable<TopKCustomKey> values, Context context) throws IOException, InterruptedException {
    	for (TopKCustomKey value : values) {
    			
    		TopKCustomKey newCustomKey = new TopKCustomKey();
    		newCustomKey.setPageName(value.getPageName());
    		newCustomKey.setPageRankValue(value.getPageRankValue());
    		
    		recordSet.add(newCustomKey);
    		// Similar as mapper, removes the first element if count exceeds 100, 
    		// since it is guaranteed first value will be smallest in Tree set.
			if (recordSet.size() > 100) {
				recordSet.remove(recordSet.first());
			}
		}

    	// At this point set has the top-100 pages with highest page rank values in ascending order
    	// Using the descendingSet to reverse the Set and emit the pages with highest page rank first. 
		for (TopKCustomKey t : recordSet.descendingSet()) {
			context.write(NullWritable.get(), t);
		}
	}

   }

