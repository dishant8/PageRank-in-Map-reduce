package hw3;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



// It is used to create a composite key to of pageName and page rank, and pass it to tree set,
// which in turn calls the compareTo method which is compared on basis of the page rank.
public class TopKCustomKey implements WritableComparable<TopKCustomKey> {

	private String pageName;
	private Double pageRankValue;
	public void write(DataOutput out) throws IOException {
		out.writeUTF(pageName);
		out.writeDouble(pageRankValue);		
	}
	public void readFields(DataInput in) throws IOException {
		this.pageName = in.readUTF();
		this.pageRankValue = in.readDouble();
		
	}
	public int compareTo(TopKCustomKey o) {
		return pageRankValue.compareTo(o.pageRankValue);			
		
	}
	
	public String getPageName() {
		return pageName;
	}
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}
	public Double getPageRankValue() {
		return pageRankValue;
	}
	public void setPageRankValue(Double pageRankValue) {
		this.pageRankValue = pageRankValue;
	}
	@Override
	public String toString() {
		return "pageName = " + pageName + ", pageRankValue = " + pageRankValue ;
	}
	
	
	
}
