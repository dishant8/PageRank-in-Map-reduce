package matrixColumnByRow;
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

//			context.getCounter(Driver.COUNTER.NUMBEROFPAGES).increment(1);
			Text pageName = new Text(afterParsed.getPageName());
			List<String> adjacencyList = afterParsed.getAdjacencylist();
			
			for(int i = 0; i < afterParsed.getAdjacencylist().size(); i++){
				context.write(new Text(adjacencyList.get(i)),
						new InlinkAndContribution(1.0/adjacencyList.size(),afterParsed.getPageName(), 0));
			}
			context.write(new Text(afterParsed.getPageName()),
						new InlinkAndContribution(0.0, "", adjacencyList.size()));
		}
    }	
}
