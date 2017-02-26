package hw3;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.xml.sax.SAXException;

// Mapper for parsing job which takes .bz2 file as input, decompresses 
// it and sends each line of file to the map call

public class MapperForParser extends Mapper<Object, Text, Text, Node>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Bz2WikiParser parsing = new Bz2WikiParser();
		Node afterParsed = new Node();
		try {
			// checks and return value only if name contents are legal that is does 
			// not contain ~ and keeps only the name of the link    
			afterParsed = parsing.doParssing(value.toString());
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(afterParsed != null){    			
			int numberOfPagesCount = Integer.parseInt(context.getConfiguration().get("numberOfPages")) + 1;
			
			context.getConfiguration().set("numberOfPages",	numberOfPagesCount + "");
			context.getCounter(Driver.COUNTER.NUMBEROFPAGES).increment(1);
			Text pageName = new Text(afterParsed.getPageName());
			Node valueAndList = new Node(-99.0, afterParsed.getAdjacencylist(), "");
			context.write(pageName, valueAndList);
		}
		
	   
    }
	

}
