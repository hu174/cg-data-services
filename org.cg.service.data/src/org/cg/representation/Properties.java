package org.cg.representation;

public class Properties {
	private int corePoolSize ;
	private int maximumPoolSize ;
	private long keepAliveTime ;
	private int queueSize ;
	private String table ;
	private String instance ;
	private String tableLayout ;
	public int getCorePoolSize() {
		return corePoolSize;
	}
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}
	public int getMaximumPoolSize() {
		return maximumPoolSize;
	}
	public void setMaximumPoolSize(int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
	}
	public long getKeepAliveTime() {
		return keepAliveTime;
	}
	public void setKeepAliveTime(long keepAliveTime) {
		this.keepAliveTime = keepAliveTime;
	}
	public int getQueueSize() {
		return queueSize;
	}
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getInstance() {
		return instance;
	}
	public void setInstance(String instance) {
		this.instance = instance;
	}
	public String getTableLayout() {
		return tableLayout;
	}
	public void setTableLayout(String tableLayout) {
		this.tableLayout = tableLayout;
	}
	
	
}
