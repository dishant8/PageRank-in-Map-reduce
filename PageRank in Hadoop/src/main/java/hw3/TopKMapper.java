package hw3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Reference - Used the reference of the Top-K job from the module 

// TopKMapper uses the tree set to keep the page rank value in the ascending order 
// It emits only 100 largest page rank values for each map call. This should work because if the record 
// is in the global top-100 it has to be in corresponding local top-100

public class TopKMapper extends Mapper<Object , Text, NullWritable, TopKCustomKey>{	
	private TreeSet<TopKCustomKey> pageRankSet = new TreeSet<TopKCustomKey>();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] pageNameAndValue = value.toString().split("\t");
		
		String pageName = pageNameAndValue[0].toString();
		
		String[] rankAndList = pageNameAndValue[1].split("~");
		Double rankValue = Double.parseDouble(rankAndList[0].toString());
		TopKCustomKey customKey = new TopKCustomKey();
		customKey.setPageName(pageName);
		customKey.setPageRankValue(rankValue);
		pageRankSet.add(customKey);

		// removes the first element if count exceeds 100, since it is guaranteed first 
		// value will be smallest in Tree set.
		if (pageRankSet.size() > 100) {
			pageRankSet.remove(pageRankSet.first());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
//		Emits the local top-100 pages with highest page rank for this mapper
		
		for (TopKCustomKey t : pageRankSet) {
			context.write(NullWritable.get(), t);
		}
	}

}

