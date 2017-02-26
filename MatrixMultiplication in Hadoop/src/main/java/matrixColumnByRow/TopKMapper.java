package matrixColumnByRow;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Reference - Used the reference of the Top-K job from the module 

// TopKMapper uses the tree set to keep the page rank value in the ascending order 
// It emits only 100 largest page rank values for each map call. This should work because if the record 
// is in the global top-100 it has to be in corresponding local top-100

public class TopKMapper extends Mapper<Object , Text, NullWritable, TopKCustomKey>{	
	private TreeSet<TopKCustomKey> pageRankSet = new TreeSet<TopKCustomKey>();

	Map<Integer,String> tracker;
	@Override
	protected void setup(Mapper<Object, Text, NullWritable, TopKCustomKey>.Context context)
			throws IOException, InterruptedException {
		
		tracker = new HashMap<Integer, String>();
//		FileSystem fs = FileSystem.get(context.getConfiguration());
//		Path[] localPaths = context.getLocalCacheFiles();		
//		BufferedReader bf2 = new BufferedReader(new InputStreamReader(fs.open(localPaths[0])));
		BufferedReader bf2 = new BufferedReader(new FileReader("./tracking-r-00000"));
		String line2;
		
		while((line2 = bf2.readLine()) != null){
			String[] nameAndReference = line2.toString().split("\t");
			tracker.put(Integer.parseInt(nameAndReference[1]), nameAndReference[0]);
		}		
		
	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] pageNameAndValue = value.toString().split("\t");
		
		String pageRefernece = pageNameAndValue[0].toString();
		System.out.println("TRACKER SIZEEEEEEEEEEE  " + tracker.size());
		String pageName = tracker.get(Integer.parseInt(pageRefernece));
		
		Double rankValue = Double.parseDouble(pageNameAndValue[1]);
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

