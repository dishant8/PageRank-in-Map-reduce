package matrix;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Custom object which is used to store the page rank value,page name to distinguish graph and 
// contributions at the reducer and adjacency list of the page

public class Node implements Writable{	
	private Double pageRank;
	private List<String> adjacencylist;
	private String pageName;
	
	
	public Double getPageRank() {
		return pageRank;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public List<String> getAdjacencylist() {
		return adjacencylist;
	}

	public void setAdjacencylist(List<String> adjacencylist) {
		this.adjacencylist = adjacencylist;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	
	
	public Node(Double pageRank, List<String> adjacencylist, String pageName) {
		super();
		this.pageRank = pageRank;
		this.adjacencylist = adjacencylist;
		this.pageName = pageName;
	}

	
	public Node() {
		super();
	}

	public void readFields(DataInput arg0) throws IOException {
		this.pageRank  = arg0.readDouble();
		int count = arg0.readInt();
		this.pageName = arg0.readUTF();
		adjacencylist = new ArrayList<String>();
		for(int i = 0; i < count; i ++){
			this.adjacencylist.add(arg0.readUTF());			
		}	
	}
	
	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(pageRank);
		arg0.writeInt(adjacencylist.size());
		arg0.writeUTF(pageName);
		for(String term: adjacencylist){
			arg0.writeUTF(term);			
		}
	}

	@Override
	public String toString() {
		String join = "";
		if(null != adjacencylist){
			join = String.join(",", adjacencylist);
		}
//		return pageRank + "~" + join ;
		return join;
	}	

}
