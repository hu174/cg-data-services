package org.cg.representation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response.Status;

import org.cg.resources.RangeScanRowsResource.RowStreamer;
import org.kiji.rest.KijiClient;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

import com.google.common.collect.Lists;

public class QueryWorker extends Thread {
	private QueryTask task;
	private Response response;
	private Long timeMillis;
	
	public Long getTimeMillis() {
		return timeMillis;
	}

	public void setTimeMillis(Long timeMillis) {
		this.timeMillis = timeMillis;
	}

	public Response getResponse() {
		return response;
	}

	public void setResponse(Response response) {
		this.response = response;
	}

	public QueryTask getTask() {
		return task;
	}

	public void setTask(QueryTask task) {
		this.task = task;
	}

	protected final List<KijiColumnName> addColumnDefs(
			KijiTableLayout tableLayout, ColumnsDef columnsDef,
			String requestedColumns) {

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
					Map<String, ColumnLayout> groupColMap = layout
							.getColumnMap();
					if (kijiColumn.isFullyQualified()) {
						ColumnLayout groupColLayout = groupColMap
								.get(kijiColumn.getQualifier());
						if (null != groupColLayout) {
							columnsDef.add(kijiColumn);
							returnCols.add(kijiColumn);
						}
					} else {
						for (ColumnLayout c : groupColMap.values()) {
							KijiColumnName fullyQualifiedGroupCol = new KijiColumnName(
									kijiColumn.getFamily(), c.getName());
							columnsDef.add(fullyQualifiedGroupCol);
							returnCols.add(fullyQualifiedGroupCol);
						}
					}
				}
			}
		}

		if (returnCols.isEmpty()) {
			throw new WebApplicationException(new IllegalArgumentException(
					"No columns selected!"), Status.BAD_REQUEST);
		}

		return returnCols;
	}

	public void close() {
		Thread.currentThread().interrupt();
	}

	@Override
	public void run() {
		while (!Thread.currentThread().isInterrupted()) {
			Iterable<KijiRowData> scanner = task.getScanner();
			KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
			ColumnsDef colsRequested = dataBuilder.newColumnsDef()
					.withMaxVersions(task.getVersion());
			List<KijiColumnName> requestedColumns = addColumnDefs(
					task.getKijiClient()
							.getKijiTable(task.getInstance(), task.getTable())
							.getLayout(), colsRequested, task.getColumns());
			response = Response.ok(
					new RowStreamer(scanner, task.getKijiClient()
							.getKijiTable(task.getInstance(), task.getTable()), task.getLimit(), requestedColumns))
					.build();
		}
	}

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
		        while (it.hasNext() && (num < numRows || numRows == -1)
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
