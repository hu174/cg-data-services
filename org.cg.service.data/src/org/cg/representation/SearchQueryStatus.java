package org.cg.representation;

public class SearchQueryStatus {
	/**
	 * {"searchQueryStatus":{"state":3,
	 * "searchedTimeRange":{"endMillis":1382576402569,"startMillis":1382563802569},
	 * "counts":[{"count":0,"alias":"raw.count"},{"count":0,"alias":"query.count"}],
	 * "buckets":[{"alias":"histogram","buckets":[]}],
	 * "pendingMessageLocatorsAndOffsets":[],
	 * "pendingErrors":null,"pendingWarnings":[],
	 * "sessionIdString":"13A02C6F523298B7"},
	 * "error":false,
	 * "errorMessage":null,
	 * "keyedErrors":[],
	 * "errorInstanceId":null,
	 * "errorKey":null}
	 */
	private SearchedTimeRange searchedTimeRange;
	private String sessionIdString;
	private boolean isError;
	private String errorInstanceId;
	private String errorMessage;
	private boolean isFinished;
	
	public SearchQueryStatus(SearchedTimeRange searchedTimeRange,
			String sessionIdString, boolean isError, String errorInstanceId,
			String errorMessage, boolean isFinished) {
		this.searchedTimeRange = searchedTimeRange;
		this.sessionIdString = sessionIdString;
		this.isError = isError;
		this.errorInstanceId = errorInstanceId;
		this.errorMessage = errorMessage;
		this.isFinished = isFinished;
	}
	
	
	public boolean isFinished() {
		return isFinished;
	}


	public void setFinished(boolean isFinished) {
		this.isFinished = isFinished;
	}


	public SearchedTimeRange getSearchedTimeRange() {
		return searchedTimeRange;
	}
	public void setSearchedTimeRange(SearchedTimeRange searchedTimeRange) {
		this.searchedTimeRange = searchedTimeRange;
	}
	public String getSessionIdString() {
		return sessionIdString;
	}
	public void setSessionIdString(String sessionIdString) {
		this.sessionIdString = sessionIdString;
	}
	public boolean isError() {
		return isError;
	}
	public void setError(boolean isError) {
		this.isError = isError;
	}
	public String getErrorInstanceId() {
		return errorInstanceId;
	}
	public void setErrorInstanceId(String errorInstanceId) {
		this.errorInstanceId = errorInstanceId;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
