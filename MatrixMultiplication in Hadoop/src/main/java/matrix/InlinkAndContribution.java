package matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

// Custom class to store the inLink, contribution and size of its adjacency list.

public class InlinkAndContribution implements Writable{

	private Double contribution;
	private String inLink;
	private int sizeOfAdjacency;	
	
	
	
	public Double getContribution() {
		return contribution;
	}

	public void setContribution(Double contribution) {
		this.contribution = contribution;
	}

	public String getInLink() {
		return inLink;
	}

	public void setInLink(String inLink) {
		this.inLink = inLink;
	}

	public int getSizeOfAdjacency() {
		return sizeOfAdjacency;
	}

	public void setSizeOfAdjacency(int sizeOfAdjacency) {
		this.sizeOfAdjacency = sizeOfAdjacency;
	}
	
	public InlinkAndContribution() {
		super();
	}

	public InlinkAndContribution(Double contribution, String inLink, int sizeOfAdjacency) {
		super();
		this.contribution = contribution;
		this.inLink = inLink;
		this.sizeOfAdjacency = sizeOfAdjacency;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(contribution);
		out.writeUTF(inLink);
		out.writeInt((sizeOfAdjacency));;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.contribution  = in.readDouble();
		this.inLink = in.readUTF();
		this.sizeOfAdjacency = in.readInt();
	}
}
