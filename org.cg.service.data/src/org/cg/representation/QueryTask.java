package org.cg.representation;

import org.kiji.rest.KijiClient;
import org.kiji.schema.KijiRowData;

public class QueryTask {
	private Iterable<KijiRowData> scanner;
	private SearchQueryStatus status;
	private final KijiClient kijiClient;
	private int version;
	private int limit;
	private String columns;
	private String instance;
	private String table;
	
	public QueryTask(Iterable<KijiRowData> _scanner, SearchQueryStatus status,
			KijiClient kijiClient, int version, int limit, String columns,
			String instance, String table) {
		scanner = _scanner;
		this.status = status;
		this.kijiClient = kijiClient;
		this.version = version;
		this.limit = limit;
		this.columns = columns;
		this.instance = instance;
		this.table = table;
	}

	public QueryTask(KijiClient _kijiClient){
		kijiClient = _kijiClient;
	}

	public Iterable<KijiRowData> getScanner() {
		return scanner;
	}

	public void setScanner(Iterable<KijiRowData> _scanner) {
		scanner = _scanner;
	}

	public SearchQueryStatus getStatus() {
		return status;
	}

	public void setStatus(SearchQueryStatus status) {
		this.status = status;
	}

	public KijiClient getKijiClient() {
		return kijiClient;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
	
}
