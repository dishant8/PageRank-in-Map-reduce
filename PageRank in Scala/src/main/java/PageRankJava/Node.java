package PageRankJava;

/**
 * Created by Dishant on 11/6/16.
 */

/**
 * Created by Dishant on 11/4/16.
 */

        import java.io.DataInput;
        import java.io.DataOutput;
        import java.io.IOException;
        import java.io.Serializable;
        import java.util.ArrayList;
        import java.util.List;

        import org.apache.hadoop.io.ArrayWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.io.Writable;

// Custom object which is used to store the page rank value,page name to distinguish graph and
// contributions at the reducer and adjacency list of the page
public class Node implements Serializable{


    private String pageName;
    private List<String> adjacencylist;

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public List<String> getAdjacencylist() {
        return adjacencylist;
    }

    public void setAdjacencylist(List<String> adjacencylist) {
        this.adjacencylist = adjacencylist;
    }


    public Node(String pageName, List<String> adjacencylist) {
        this.pageName = pageName;
        this.adjacencylist = adjacencylist;
    }

    public Node() {
    }

    @Override
    public String toString() {
        String join = "";
//		System.out.println("ADJACENCY VALUE" + adjacencylist);
        if(null != adjacencylist){
            join = String.join(",", adjacencylist);
        }
        return pageName + "~" + join ;
//		return pageRank + "~" + adjacencylist.toString() ;
//		+ "~" + pageName
    }

}

