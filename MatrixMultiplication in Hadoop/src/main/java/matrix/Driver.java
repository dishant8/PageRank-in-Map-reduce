package matrix;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Driver class which initializes three types of jobs, i.e Job for parsing the html files,
// calculate the page rank and calculate the Top K values for page rank

public class Driver {
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		// Setting up jobs
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Job configuration to do initial parsing of html Data
        parseInput(conf, otherArgs);

        // 10 iterations to calculate the page rank
        for(int i = 0; i < 10; i ++){
        	conf.setInt("iterations", i);
    		pageRankJob(conf);
        }
        // Map reduce job to calculate the 100 top pages from the above iterations
        topK(conf, otherArgs);        
	}
	
	private static void parseInput(Configuration conf, String[] otherArgs)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Job job = new Job(conf, "parseInput");
        job.setJarByClass(Driver.class);
        // Setting up mapper and reducer class
        job.setMapperClass(MapperForParser.class);
        job.setReducerClass(ReducerForParser.class);        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InlinkAndContribution.class);
        // Setting number of reduce task to 1.
        job.setNumReduceTasks(1);
        for (int i = 0; i < otherArgs.length -1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path("RowByColumn/Data0"));

        // Configuring multiple outputs for writing multiple files in same job.
		MultipleOutputs.addNamedOutput(job, "Tracking", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "OutputFile", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "DanglingFile", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "PageRankVector", TextOutputFormat.class, Text.class, Text.class);
        job.waitForCompletion(true);        
        
        // Set the value of constants generated in this job in the configuration object to pass it to the next job 
        conf.setLong("numberOfPages", job.getCounters().findCounter(COUNTER.NUMBEROFPAGES).getValue());
        conf.setLong("damplingNode", job.getCounters().findCounter(COUNTER.DANGLINGNODE).getValue());
	}

	// Job to calculate the page rank, which gets called iteratively.
	private static void pageRankJob(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {		
		int iteration = conf.getInt("iterations", -10);
		Job job = new Job(conf, "pageRank");
        job.setJarByClass(Driver.class);
        
        // Adding files to distributed cache.
        job.addCacheFile(new URI("RowByColumn/Data0/dangling-r-00000"));
        job.addCacheFile(new URI("RowByColumn/Data0/pageRank" + iteration));
        // Setting up Mapper 
        job.setMapperClass(PageRankMapper.class);

        // Setting output key and value for reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path("RowByColumn/Data0/contributions-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("RowByColumn/Data0/pageRank" + (iteration + 1)));        
        job.waitForCompletion(true);
	}
	

	
	// Job to calculate the top 100 pages after iterating ten times to calculate the page rank
	private static void topK(Configuration conf, String[] otherArgs) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		int iteration = conf.getInt("iterations", -10);
		Job job = new Job(conf, "pageRank");
        job.addCacheFile(new URI("RowByColumn/Data0/tracking-r-00000"));
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
        FileInputFormat.addInputPath(job, new Path("RowByColumn/Data0/pageRank" + (iteration + 1)));

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
