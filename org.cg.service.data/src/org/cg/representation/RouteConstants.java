package org.cg.representation;

public class RouteConstants {
	public static final String scanPath = "/v1/instances/{instance}/tables/{table}/scan";
	public static final String searchPath = "/v1/instances/{instance}/tables/{table}/searchquery";
	public static final String statusPath = "/v1/instances/{instance}/tables/{table}/queryId/{queryId}/status";
	public static final String rawPath = "/v1/instances/{instance}/tables/{table}/queryId/{queryId}/raw";
	public static final String aggregatePath = "/v1/instances/{instance}/tables/{table}/queryId/{queryId}/records";
	public static final int _capacity = 100;
}
