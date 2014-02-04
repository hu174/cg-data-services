package org.cg.resources;

import static org.cg.representation.RouteConstants.*;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.Status;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.HConstants;
import org.cg.filters.ColumnValueGreaterEqualsRowFilter;
import org.kiji.rest.KijiClient;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.filter.AndRowFilter;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ResourceUtils;

import com.google.common.collect.Lists;


@Path(scanPath) 
@Produces(MediaType.APPLICATION_JSON)
public class RangeScanRowsResource {
	private static final int UNLIMITED_ROWS = -1;
	private static final String UNLIMITED_VERSIONS = "all";
	private Method m;
	private static KijiClient kijiClient;
	
	RangeScanRowsResource(KijiClient _kijiClient){
		kijiClient = _kijiClient;
	}
	 
	 /**
	 * According target name to parse keyFormat, get total number of components
	 * @param keyFormat Key Format get from TableLayoutDesc
	 * @param target Here is "components"
	 * @return	number of components in key
	 */
	public int getComponentsNum(String keyFormat, String target) {
		int index = keyFormat.lastIndexOf(target);
		String sub = keyFormat.substring(index + target.length() + 1,
				keyFormat.length() - 1);
		int count = 1;
		String[] strArray = sub.split(",");
		for (String str : strArray) {
			if (str.charAt(str.length() - 1) == '}')
				count++;
		}
		return count;
	}

	/**
	 * set up start/stop row key based on input key string and data types
	 * @param key	input key string get from Query Parameter
	 * @param rowKey Object array of output rowKey
	 * @param types	String array of data types
	 * @param index	
	 */
	public void setRowKey(String key, Object[] rowKey, String[] types, int index) {
		if (types[index].equals("String")){
			rowKey[index] = key;
		}
		else if (types[index].equals("Long"))
			rowKey[index] = Long.parseLong(key);
		else if (types[index].equals("Integer"))
			rowKey[index] = Integer.parseInt(key);
	}

	/**
	 * Get a list of Kiji rows
	 * @param instance is the instance where the table resides.
	 * @param table is the table where the rows from which the rows will be streamed
	 * @param limit	the maximum number of rows to return. Set to -1 to stream all rows.
	 * @param columns	is a comma separated list of columns (either family or family:qualifier) to fetch
	 * @param maxVersionsString	is the max versions per column to return. Can be "all" for all versions.
	 * @param startCompKey	starting row key
	 * @param stopCompKey	stopping row key
	 * @param keyTypes	data types of components in key
	 * @return	the Response object containing the rows requested 
	 */
	@GET
	public Response getScanedRows(
			@PathParam(INSTANCE_PARAMETER) String instance,
			@PathParam(TABLE_PARAMETER) String table,
			@QueryParam("limit") @DefaultValue("-1") int limit,
			@QueryParam("cols") @DefaultValue("*") String columns,
			@QueryParam("versions") @DefaultValue("1") String maxVersionsString,
			@QueryParam("startCompKey") String startCompKey,
			@QueryParam("stopCompKey") String stopCompKey,
			@QueryParam("keyTypes") String keyTypes) {
		//Open Kiji Table, get table layout
		KijiTable kijiTable = kijiClient.getKijiTable(instance, table);
		KijiTableLayout layout = kijiTable.getLayout();
		TableLayoutDesc desc = layout.getDesc();
		String keyFormat = desc.getKeysFormat().toString();
		//Calculate number of components in key 
		int componentsNum = getComponentsNum(keyFormat, "components");
		String[] types = null;
		Iterable<KijiRowData> scanner = null;
		//Request Builder
		KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
		int maxVersions;
		//Open Kiji table reader
		final KijiTableReader reader = kijiTable.openTableReader();

		try {
			if (UNLIMITED_VERSIONS.equalsIgnoreCase(maxVersionsString)) {
				maxVersions = HConstants.ALL_VERSIONS;
			} else {
				maxVersions = Integer.parseInt(maxVersionsString);
			}
		} catch (NumberFormatException nfe) {
			throw new WebApplicationException(nfe, Status.BAD_REQUEST);
		}
		
		//Get a list of requested Kiji column names
		ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
		List<KijiColumnName> requestedColumns = addColumnDefs(kijiTable.getLayout(), colsRequested, columns);
		
		//Check if key types input is correct
		if (keyTypes != null) {
			types = keyTypes.split(",");
			if (componentsNum != types.length) {
				throw new WebApplicationException(new IllegalArgumentException(
						"Ambiguous request. " + "Incorrect number of key types input."), Status.BAD_REQUEST);
			}
		} else if (startCompKey != null || stopCompKey != null) {
			throw new WebApplicationException(new IllegalArgumentException( 
					"Ambiguous request. " + "Lack of key types input."), Status.BAD_REQUEST);
		}

		try {
			final KijiScannerOptions scanOptions = new KijiScannerOptions();
			if (startCompKey != null) {
				String[] startComponents = startCompKey.split(",");
				if (componentsNum != startComponents.length) {
					throw new WebApplicationException(
							new IllegalArgumentException("Ambiguous request. "
									+ "Start Key Components number incorrect."),
							Status.BAD_REQUEST);
				}
				Object[] startRowKey = new Object[startComponents.length];
				int index = 0;
				for (String comp : startComponents) {
					setRowKey(comp, startRowKey, types, index);
					index++;
				}
				EntityId startRowId = kijiTable.getEntityId(startRowKey);
				//EntityId startRowId = (EntityId) m.invoke(kijiTable, (Object) startRowKey);
				scanOptions.setStartRow(startRowId);
			}
			if (stopCompKey != null) {
				String[] stopComponents = stopCompKey.split(",");
				if (componentsNum != stopComponents.length) {
					throw new WebApplicationException(
							new IllegalArgumentException("Ambiguous request. "
									+ "Stop Key Components number incorrect."),
							Status.BAD_REQUEST);
				}
				Object[] stopRowKey = new Object[stopComponents.length];
				int index = 0;
				for (String comp : stopComponents) {
					setRowKey(comp, stopRowKey, types, index);
					index++;
				}
				EntityId stopRowId = kijiTable.getEntityId(stopRowKey);
				//EntityId stopRowId = (EntityId) m.invoke(kijiTable, (Object) stopRowKey);
				scanOptions.setStopRow(stopRowId);
			}
			/**
			Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
			DecodedCell<Integer> value1 = new DecodedCell<Integer>(SCHEMA_INT,2241244);
			ColumnValueGreaterEqualsRowFilter equalFilter1 = new ColumnValueGreaterEqualsRowFilter("PaymentViewStatus", "Customer_Id", value1);
			//ColumnValueEqualsRowFilter equalFilter2 = new ColumnValueEqualsRowFilter("PaymentViewStatus", "Customer_Id", value1);
			AndRowFilter andFilter = new AndRowFilter(equalFilter1);
			scanOptions.setKijiRowFilter(andFilter);
			*/
			
			scanner = reader.getScanner(dataBuilder.build(), scanOptions);
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(e, Status.BAD_REQUEST);
		} finally {
			ResourceUtils.releaseOrLog(kijiTable);
		}
		
		return Response.ok(
				new RowStreamer(scanner, kijiTable, limit, requestedColumns))
				.build();
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

	/**
	 * Class to support streaming KijiRows to the client.
	 * 
	 */
	public class RowStreamer implements StreamingOutput {

		private Iterable<KijiRowData> scanner = null;
		private final KijiTable table;
		private int numRows = 0;
		private final List<KijiColumnName> colsRequested;
		/**
		 * Construct a new RowStreamer.
		 * 
		 * @param scanner	is the iterator over KijiRowData.
		 * @param table		the table from which the rows originate.
		 * @param numRows	is the maximum number of rows to stream.
		 * @param columns	are the columns requested by the client.
		 */
		public RowStreamer(Iterable<KijiRowData> _scanner, KijiTable _table,
				int _numRows, List<KijiColumnName> _columns) {
			scanner = _scanner;
			table = _table;
			numRows = _numRows;
			colsRequested = _columns;
		}

		/**
		 * Depth first traverse KijiRowData to retrieve values corresponding to requested columns
		 * @param writer	Java IO writer
		 * @param row	KijiRowData
		 * @param pre	combination of values get from parents' nodes
		 * @param index	level of the tree
		 * @param list	list of requested columns
		 */
		public void writerHelper(Writer writer, KijiRowData row, StringBuffer pre, int index, List<KijiColumnName> list){
			
			if(index == list.size()){
				try {
					writer.write(pre.append("\r\n").toString());
					writer.flush();
				} catch (IOException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return;
			}
			try {
				NavigableMap<Long, Object> map = row.getValues(list.get(index).getFamily(), list.get(index).getQualifier());
				Iterator<Object> iter = map.values().iterator();
				if(map.values().isEmpty())
					writerHelper(writer,row,pre.append(","),index+1,list);
				while(iter.hasNext()){
					String value = iter.next().toString();
					if(pre.length()==0)
						writerHelper(writer,row,pre.append(value),index+1,list);
					else
						writerHelper(writer,row,pre.append(",").append(value),index+1,list);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**
		 * Performs the actual streaming of the rows.
		 * @param os is the OutputStream where the results are written.
		 * @throws IOException 
		 */
		@Override
		public void write(OutputStream os) throws IOException {
		      int num = 0;
		      Writer writer = new BufferedWriter(new OutputStreamWriter(os, Charset.forName("UTF-8")));
		      StringBuffer header = new StringBuffer();
		      for(KijiColumnName colName : colsRequested){
		    	  if(header.length() == 0)
		    		  header.append(colName.getFamily()+"_"+colName.getQualifier());
		    	  else
		    		  header.append(","+colName.getFamily()+"_"+colName.getQualifier());
		      }
		      writer.write(header.toString() + "\r\n");
		      writer.flush();
		      Iterator<KijiRowData> it = scanner.iterator();
		      boolean clientClosed = false;

		      try {
		        while (it.hasNext() && (num < numRows || numRows == UNLIMITED_ROWS)
		            && !clientClosed) {
		          KijiRowData row = it.next();
		          StringBuffer buff = new StringBuffer();
		          writerHelper(writer, row,buff,0,colsRequested);
		          num++;
		        }
		      } finally {
		        if (scanner instanceof KijiRowScanner) {
		          try {
		            ((KijiRowScanner) scanner).close();
		          } catch (IOException e1) {
		            throw new WebApplicationException(e1, Status.INTERNAL_SERVER_ERROR);
		          }
		        }
		      }

		      if (!clientClosed) {
		        try {
		          writer.flush();
		          writer.close();
		        } catch (IOException e) {
		          throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
		        }
		      }
		    }
		  }
}
