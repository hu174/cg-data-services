package org.cg.resources;

import static org.cg.representation.RouteConstants.searchPath;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.util.HashMap;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.Status;

import org.cg.representation.Channel;
import org.cg.representation.PuckThreadPoolExecutor;
import org.cg.representation.QueryMetaData;
import org.cg.representation.QueryTask;
import org.cg.representation.QueryWorker;
import org.cg.representation.Request;
import org.cg.representation.Task;
import org.cg.rest.QueryManager;
import org.kiji.rest.KijiClient;

import com.fasterxml.jackson.databind.JsonNode;

@Path(searchPath) 
@Produces(MediaType.APPLICATION_JSON)
public class SearchQueryResource {
	private QueryManager queryManager;
	private KijiClient kijiClient;

	public SearchQueryResource(QueryManager _queryManager, KijiClient _kijiClient){
		queryManager = _queryManager;
		kijiClient = _kijiClient;
	}
	
	@GET
    @Produces("text/plain")
    public String getClichedMessage() {
        return "Hello World";
    }

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response post(@PathParam(INSTANCE_PARAMETER) String instance,
			@PathParam(TABLE_PARAMETER) String table,
			Request request){
		System.out.println(request);
		Properties prop = queryManager.getProperties();
		String queryId = queryManager.queryIdGenerator(instance+table+request.toString());
		Task task = new Task(kijiClient, instance, table, request, prop);
		//task.setRequest(request);
		queryManager.registerTask(queryId, task);
		//queryManager.getThreadPoolExecutor().execute(task);
		QueryMetaData meta = new QueryMetaData();
		meta.setQueryId(queryId);
		return Response.status(200).entity(meta).build();
		
	}
	
	

}
