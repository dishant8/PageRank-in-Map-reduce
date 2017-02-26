package hw3;
import java.util.List;


// Custom object which is used to store the page name and adjacency list 
public class PageNameAndValue {
	
	private String pageName;
	private List<String> value;
	public PageNameAndValue(String pageName, List<String> value) {
		super();
		this.pageName = pageName;
		this.value = value;
	}
	
	
	public PageNameAndValue() {
		super();
	}

	public String getPageName() {
		return pageName;
	}
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}
	public List<String> getValue() {
		return value;
	}
	public void setValue(List<String> value) {
		this.value = value;
	}
	
	

}
