package org.cg.representation;

public class SearchedTimeRange {
	private String startMillis;
	private String endMillis;
	
	
	public SearchedTimeRange(String startMillis, String endMillis) {
		this.startMillis = startMillis;
		this.endMillis = endMillis;
	}
	
	public String getStartMillis() {
		return startMillis;
	}
	public void setStartMillis(String startMillis) {
		this.startMillis = startMillis;
	}
	public String getEndMillis() {
		return endMillis;
	}
	public void setEndMillis(String endMillis) {
		this.endMillis = endMillis;
	}
}
