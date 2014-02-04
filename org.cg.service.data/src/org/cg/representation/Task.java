package org.cg.representation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;
import org.cg.rest.KijiFactory;
import org.cg.rest.QueryManager;
import org.cg.rest.ResponseBuilder;
import org.cg.rest.ScanOptionsFactory;
import org.kiji.rest.KijiClient;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

import com.google.common.collect.Lists;

/**
 * 
 * @author yingkaihu
 *
 */
public class Task implements Runnable {
	private SearchQueryStatus status;
	private Iterator operatorChain;
	private DataSource input;
	private DataSource output;
	private Request request;
	private KijiClient kijiClient;
	private String instance;
	private String table;
	private Properties prop;
	private static final Logger log = Logger.getLogger(Task.class);

	private static final String INSTANCE = "instance";
	private static final String TABLE = "table";
	/**
	 * 
	 * @param _kijiClient
	 * @param _instance
	 * @param _table
	 * @param _request
	 */
	public Task(KijiClient _kijiClient, String _instance, String _table, Request _request, Properties _prop){
		kijiClient = _kijiClient;
		instance = _instance;
		table = _table;
		request = _request;
		prop = _prop;
	}
	
	public Request getRequest() {
		return request;
	}

	public void setRequest(Request request) {
		this.request = request;
	}
	/**
	 * 
	 */
	@Override
	public void run() {
		KijiScannerOptions scanOptions = ScanOptionsFactory.buildScanOptions(kijiClient, instance, table, request);
		KijiTable kijiTable = kijiClient.getKijiTable(instance, table);
		final KijiTableReader reader = kijiTable.openTableReader();
		KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
		//Get a list of requested Kiji column names
		ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(Integer.valueOf(request.getMaxVersion()));
		List<KijiColumnName> requestedColumns = addColumnDefs(kijiTable.getLayout(), colsRequested, request.getColumns());
		Iterable<KijiRowData> scanner = null;
		try {
			 scanner = reader.getScanner(dataBuilder.build(), scanOptions);
		} catch (IOException e) {
			log.error(e.getMessage(),e);
			e.printStackTrace();
		}
		Response response = ResponseBuilder.build(scanner, kijiTable, Integer.valueOf(request.getLimit()), requestedColumns);
		
	}
	
	/**
	   * Returns a list of fully qualified KijiColumnNames to return to the client.
	   *
	   * @param tableLayout is the layout of the table from which the row is being fetched.
	   *
	   * @param columnsDef is the columns definition object being modified to be passed down to the
	   *        KijiTableReader.
	   * @param requestedColumns the list of user requested columns to display.
	   * @return the list of KijiColumns that will ultimately be displayed. Since this method validates
	   *         the list of incoming columns, it's not necessarily the case that what was specified in
	   *         the requestedColumns string correspond exactly to the list of outgoing columns. In some
	   *         cases it could be less (in case of an invalid column/qualifier) or more in case of
	   *         specifying only the family but no qualifiers.
	   */
	  protected final List<KijiColumnName> addColumnDefs(KijiTableLayout tableLayout,
	      ColumnsDef columnsDef, String requestedColumns) {

	    List<KijiColumnName> returnCols = Lists.newArrayList();
	    Collection<KijiColumnName> requestedColumnList = null;
	    // Check for whether or not *all* columns were requested
	    if (requestedColumns == null || requestedColumns.trim().equals("*")) {
	      requestedColumnList = tableLayout.getColumnNames();
	    } else {
	      requestedColumnList = Lists.newArrayList();
	      String[] requestedColumnArray = requestedColumns.split(",");
	      for (String s : requestedColumnArray) {
	        requestedColumnList.add(new KijiColumnName(s));
	      }
	    }

	    Map<String, FamilyLayout> colMap = tableLayout.getFamilyMap();
	    // Loop over the columns requested and validate that they exist and/or
	    // expand qualifiers
	    // in case only family names were specified (in the case of group type
	    // families).
	    for (KijiColumnName kijiColumn : requestedColumnList) {
	      FamilyLayout layout = colMap.get(kijiColumn.getFamily());
	      if (null != layout) {
	        if (layout.isMapType()) {
	          columnsDef.add(kijiColumn);
	          returnCols.add(kijiColumn);
	        } else {
	          Map<String, ColumnLayout> groupColMap = layout.getColumnMap();
	          if (kijiColumn.isFullyQualified()) {
	            ColumnLayout groupColLayout = groupColMap.get(kijiColumn.getQualifier());
	            if (null != groupColLayout) {
	              columnsDef.add(kijiColumn);
	              returnCols.add(kijiColumn);
	            }
	          } else {
	            for (ColumnLayout c : groupColMap.values()) {
	              KijiColumnName fullyQualifiedGroupCol = new KijiColumnName(kijiColumn.getFamily(),
	                  c.getName());
	              columnsDef.add(fullyQualifiedGroupCol);
	              returnCols.add(fullyQualifiedGroupCol);
	            }
	          }
	        }
	      }
	    }

	    if (returnCols.isEmpty()) {
	      throw new WebApplicationException(new IllegalArgumentException("No columns selected!"),
	          Status.BAD_REQUEST);
	    }

	    return returnCols;
	  }
}
