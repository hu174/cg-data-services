package org.cg.representation;

import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * 
 * @author yingkaihu
 *
 *@PathParam(INSTANCE_PARAMETER) String instance,
			@PathParam(TABLE_PARAMETER) String table,
			@QueryParam("limit") @DefaultValue("-1") int limit,
			@QueryParam("cols") @DefaultValue("*") String columns,
			@QueryParam("versions") @DefaultValue("1") String maxVersionsString,
			@QueryParam("startCompKey") String startCompKey,
			@QueryParam("stopCompKey") String stopCompKey,
			@QueryParam("keyTypes") String keyTypes
 */
public class Request {
	private String instance;
	private String table;
	private String limit;
	private String columns;
	private String maxVersion;
	private String startCompKey;
	private String endCompKey;
	private String keyTypes;
	private String filterList;
	private List<String> filters;
	
	private String startMillis;
	private String endMillis;
	private String timeRange;
	private String timezone;
	
	
	
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
	public List<String> getFilters() {
		if(filterList != null){
			String[] string = filterList.split(",");
			for(String str : string){
				filters.add(str);
			}
			return filters;
		} 
		else
			return null;
	}
	public void setFilters(List<String> filters) {
		this.filters = filters;
	}
	public String getFilterList() {
		return filterList;
	}
	public void setFilterList(String filterList) {
		this.filterList = filterList;
	}
	public String getMaxVersion() {
		return maxVersion;
	}
	public void setMaxVersion(String maxVersion) {
		this.maxVersion = maxVersion;
	}
	public String getLimit() {
		return limit;
	}
	public void setLimit(String limit) {
		this.limit = limit;
	}
	public String getStartCompKey() {
		return startCompKey;
	}
	public void setStartCompKey(String startCompKey) {
		this.startCompKey = startCompKey;
	}
	public String getEndCompKey() {
		return endCompKey;
	}
	public void setEndCompKey(String endCompKey) {
		this.endCompKey = endCompKey;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getKeyTypes() {
		return keyTypes;
	}
	public void setKeyTypes(String keyTypes) {
		this.keyTypes = keyTypes;
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
	public String getTimeRange() {
		return timeRange;
	}
	public void setTimeRange(String timeRange) {
		this.timeRange = timeRange;
	}
	public String getTimezone() {
		return timezone;
	}
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}
	
	public String toString(){
		return startCompKey+endCompKey+keyTypes+columns;
	}
	
}
