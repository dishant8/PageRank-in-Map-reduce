package hw3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xml.sax.SAXException;

// Driver class which initializes three types of jobs, i.e Job for parsing the html files,
// calculate the page rank and calculate the Top K values for page rank

public class Driver {
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Setting up three types of jobs
		Configuration conf = new Configuration();

		conf.set("numberOfPages", "0");		
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.out.println(otherArgs.length);
        // Job configuration to do initial parsing of html Data
        parseInput(conf, otherArgs);

        
        // 11 different iterations of MR job to calculate the page rank, out of which first iteration 
        // accounts to assign the initial page rank value and calculate summation of Dangling nodes, 
        // so that during the fist iteration it can account for dangling nodes along with updating the page rank.
        for(int i = 0; i < 11; i ++){
        	conf.setInt("itr", i);
    		pageRankJob(conf);
        }
        
        // Map reduce job to calculate the 100 top pages from the above iterations
        topK(conf,otherArgs);

	}


	private static void parseInput(Configuration conf, String[] otherArgs)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "parseInput");
        job.setJarByClass(Driver.class);
        // Setting up Map only class 
        job.setMapperClass(MapperForParser.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path("output/Data0"));

        job.waitForCompletion(true);        
        // Set the value of constants generated in this job in the configuration object to pass it to the next job 
        conf.setLong("numberOfPages", job.getCounters().findCounter(COUNTER.NUMBEROFPAGES).getValue());
        conf.setLong("damplingNode", job.getCounters().findCounter(COUNTER.DANGLINGNODE).getValue());
	}

	
	// Job to calculate the page rank, which gets called iteratively.
	private static void pageRankJob(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {		
		int iteration = conf.getInt("itr", -10);
		System.out.println("CHECK FOR COUNTER"  + conf.getLong("numberOfPages", -10));
		Job job = new Job(conf, "pageRank");
        job.setJarByClass(Driver.class);
        // Setting up Mapper and Reducer classes
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        // Setting output key and value for reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("output/Data" + iteration));
        FileOutputFormat.setOutputPath(job,
                new Path("output/Data" + (iteration + 1)));
        job.waitForCompletion(true);
        conf.setLong("danglingNode", job.getCounters().findCounter(COUNTER.DANGLINGNODE).getValue());
        job.getCounters().findCounter(COUNTER.DANGLINGNODE).setValue(0);
	}

	    
	// Job to calculate the top 100 pages after iterating ten times to calculate the page rank
	private static void topK(Configuration conf, String[] otherArgs) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		int iteration = conf.getInt("itr", -10);
		System.out.println(iteration);
		Job job = new Job(conf, "pageRank");
        job.setJarByClass(Driver.class);
        // Setting up Mapper and Reducer classes
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);

        // Setting output key and value for reducer
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TopKCustomKey.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TopKCustomKey.class);
        job.setNumReduceTasks(1);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("output/Data" + (iteration + 1)));

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1] + "/TopK"));
        job.waitForCompletion(true);
		
	}

    // Global Constants to pass number of pages as well as updated dangling node across different iterations 
	 public static enum COUNTER {
	    	NUMBEROFPAGES,
	    	DANGLINGNODE,
	};

}
