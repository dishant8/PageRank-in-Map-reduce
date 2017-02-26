package matrix;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;

// Mapper for parsing job which takes .bz2 file as input, decompresses 
// it and sends each line of file to the map call

public class MapperForParser extends Mapper<Object, Text, Text, InlinkAndContribution>{

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
			List<String> adjacencyList = afterParsed.getAdjacencylist();
			// For each of the adjacency element emits adjacency element as key with contribution, size and  
			// page to which it is an outlink as value
			for(int i = 0; i < afterParsed.getAdjacencylist().size(); i++){
				// In this case emitting size as 0, to find the dangling node in the reducer 
				context.write(new Text(adjacencyList.get(i)),
						new InlinkAndContribution(1.0/adjacencyList.size(),afterParsed.getPageName(), 0));
			}
			context.write(new Text(afterParsed.getPageName()),
						new InlinkAndContribution(0.0, "", adjacencyList.size()));
		}
    }	
}
