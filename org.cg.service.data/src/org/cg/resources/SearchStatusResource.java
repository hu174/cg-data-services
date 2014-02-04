package org.cg.resources;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.cg.representation.SearchQueryStatus;
import org.cg.representation.SearchedTimeRange;
import org.cg.representation.Task;
import org.cg.rest.QueryManager;

import static org.cg.representation.RouteConstants.statusPath;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

@Path(statusPath)
@Produces(MediaType.APPLICATION_JSON)

public class SearchStatusResource {
	private QueryManager queryManager;
	private volatile int count;
	private static final long mockStart = 1382563802569L;
	public SearchStatusResource(QueryManager _queryManager) {
		this.queryManager = _queryManager;
		count=5;
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public synchronized Response getStatus(
			@PathParam(INSTANCE_PARAMETER) String instance,
			@PathParam(TABLE_PARAMETER) String table,
			@PathParam("queryId") String queryId) {
		//Task task = queryManager.getMetaTable().get(queryId);
		//String start = task.getRequest().getStartCompKey();
		//String end = task.getRequest().getEndCompKey();
		
		SearchQueryStatus status=null;
		synchronized(SearchStatusResource.class) {
			if(count>0){
				status = new SearchQueryStatus(new SearchedTimeRange(Long.toString(mockStart),Long.toString(mockStart+10000000*(15-count))), queryId, false, null, null, false);
				count--;
			}else if(count==0){
				status = new SearchQueryStatus(new SearchedTimeRange(Long.toString(mockStart),Long.toString(mockStart+10000000*(15-count))), queryId, false, null, null, true);
				count=5;
				
			}
			
		}
		return Response.status(200).entity(status).build();
	}
	
	public static void main(String[] args){
		System.out.println(Long.toString(mockStart));
		System.out.println(Long.toString(mockStart + 1000*5));
	}
}
