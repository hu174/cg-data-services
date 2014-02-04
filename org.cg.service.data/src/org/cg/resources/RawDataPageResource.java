package org.cg.resources;

import static org.cg.representation.RouteConstants.rawPath;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.kiji.rest.KijiClient;
import org.kiji.schema.filter.AndRowFilter;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.cg.representation.Request;
import org.cg.representation.Task;
import org.cg.rest.QueryManager;


@Path(rawPath)
@Produces(MediaType.APPLICATION_JSON)
public class RawDataPageResource {
	private QueryManager queryManager;
	private KijiClient kijiClient;
	private RangeScanRowsResource scanResource;
	
	public RawDataPageResource(QueryManager _queryManager,
			KijiClient _kijiClient) {
		this.queryManager = _queryManager;
		this.kijiClient = _kijiClient;
	}
	
	public RangeScanRowsResource getScanResource() {
		return scanResource;
	}


	public void setScanResource(RangeScanRowsResource scanResource) {
		this.scanResource = scanResource;
	}

	/**
	public KijiRowFilter getRowFilter(List<String> filters){
		List<KijiRowFilter> lists = new ArrayList<KijiRowFilter>();
		for(String str : filters){
			String[] res = str.split("=");
			String[] column = res[0].split(":");
			ColumnValueEqualsRowFilter myRowFilter = new ColumnValueEqualsRowFilter(column[0],column[1],res[1]);
		}
		
		AndRowFilter andFilter = new AndRowFilter();
	}
	*/
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRawData(@PathParam(INSTANCE_PARAMETER) String instance,
			@PathParam(TABLE_PARAMETER) String table,
			@PathParam("queryId") String queryId,
			@QueryParam("offset") @DefaultValue("0") int offset,
			@QueryParam("length") @DefaultValue("100") int length) {
		//ConcurrentHashMap<String, Task> map = queryManager.getMetaTable();
		Task task = queryManager.getMetaTable().get(queryId);
		// queryManager.getThreadPoolExecutor().execute(task);
		Request request = task.getRequest();
		List<String> filters = request.getFilters();
		
		Response response = scanResource.getScanedRows(instance, table, 1000, request.getColumns(), "1", request.getStartCompKey(), request.getEndCompKey(), request.getKeyTypes());
		
		return response;
	}
}
