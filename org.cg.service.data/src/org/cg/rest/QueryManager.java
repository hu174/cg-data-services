package org.cg.rest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.cg.representation.PuckThreadPoolExecutor;
import org.cg.representation.QueryWorker;
import org.cg.representation.SearchQueryStatus;
import org.cg.representation.Task;
import org.kiji.rest.KijiClient;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.filter.AndRowFilter;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.Resources;

/**
 * 
 * @author yingkaihu
 * 
 */
public class QueryManager {

	private static final Logger log = Logger.getLogger(QueryManager.class);

	private static final String CORE_POOL_SIZE = "corePoolSize";
	private static final String MAXIMUM_POOL_SIZE = "maximumPoolSize";
	private static final String KEEP_ALIVE_TIME = "keepAliveTime";
	private static final String QUEUE_SIZE = "queueSize";
	private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
	private static final String INSTANCE = "instance";
	private static final String TABLENAME = "table";
	private static final String TABLE_LAYOUT = "tableLayout";

	private static final int CORE_POOL_SIZE_VALUE = 10;
	private static final int MAXIMUM_POOL_SIZE_VALUE = 20;
	private static final long KEEP_ALIVE_TIME_VALUE = 10;
	private static final int QUEUE_SIZE_VALUE = 20;

	private int corePoolSize;
	private int maximumPoolSize;
	private long keepAliveTime;
	private int queueSize;

	// private static volatile QueryManager _instance;
	private volatile KijiClient kijiClient;
	private static Properties properties;
	// <qureyID,task>
	private volatile ConcurrentHashMap<String, Task> metaTable;
	private static volatile ThreadPoolExecutor threadPoolExecutor;

	
	public static Properties getProperties() {
		return properties;
	}

	private void validate() {
		Preconditions.checkArgument(corePoolSize > 0,
				"corePoolSize must be positive: %s", corePoolSize);
		Preconditions.checkArgument(maximumPoolSize >= corePoolSize,
				"maximumPoolSize must greater or equals to corePoolSize.");
		Preconditions.checkArgument(queueSize > 0,
				"queueSize must be positive: %s", queueSize);
		Preconditions.checkArgument(keepAliveTime > 0,
				"keepAliveTime must be positive: %s", keepAliveTime);
		// Preconditions.checkNotNull(unit, "TimeUnit may not be null. ");
	}

	private void setDefault() {
		queueSize = QUEUE_SIZE_VALUE;
		// unit = TIME_UNIT;
		corePoolSize = CORE_POOL_SIZE_VALUE;
		maximumPoolSize = MAXIMUM_POOL_SIZE_VALUE;
		keepAliveTime = KEEP_ALIVE_TIME_VALUE;
	}

	public void setAttribute(Properties properties) {
		corePoolSize = (Integer) properties.get(CORE_POOL_SIZE);
		maximumPoolSize = (Integer) properties.get(MAXIMUM_POOL_SIZE);
		keepAliveTime = (Long) properties.get(KEEP_ALIVE_TIME);
		queueSize = (Integer) properties.get(QUEUE_SIZE);
		try {
			validate();
		} catch (Exception e) {
			setDefault();
			log.error(e.getMessage(), e);
		}
	}

	/**
	 * initial query manager
	 * @param _kijiClient
	 * @param _properties
	 * @throws Exception
	 */
	public void init(KijiClient _kijiClient, Properties _properties){
		properties = _properties;
		kijiClient = _kijiClient;
		setAttribute(properties);
		ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<Runnable>(queueSize);
		threadPoolExecutor = new ThreadPoolExecutor(corePoolSize,
				maximumPoolSize, keepAliveTime, TIME_UNIT, workerQueue);
		metaTable = new ConcurrentHashMap<String, Task>();
		Kiji kiji = KijiFactory.open(kijiClient,
				(String) properties.get(INSTANCE));
		try {
			if (!kiji.getTableNames().contains((String) properties.get(TABLENAME))) {
				kiji.createTable(getTableLayout((String) properties
						.get(TABLE_LAYOUT)));
			}
		} catch (IOException e) {
			log.debug(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			log.debug(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public QueryManager(KijiClient _kijiClient, Properties _properties){
		init(_kijiClient, _properties);
	}

	public static ThreadPoolExecutor getThreadPoolExecutor() {
		return threadPoolExecutor;
	}

	public ConcurrentHashMap<String, Task> getMetaTable() {
		return metaTable;
	}

	public void setMetaTable(ConcurrentHashMap<String, Task> metaTable) {
		this.metaTable = metaTable;
	}

	public String queryIdGenerator(String input) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.debug(e.getMessage());
			e.printStackTrace();
		}
		md.update(input.getBytes());
		byte byteData[] = md.digest();
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
			hexString.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16)
					.substring(1));
		}
		System.out.println("Digest(in hex format):: " + hexString.toString());
		return hexString.toString();
	}

	public void registerTask(String queryId, Task task) {
		metaTable.put(queryId, task);
	}

	public static TableLayoutDesc getTableLayout(String JsonFile)
			throws Exception {
		InputStream jsonStream = Resources.openSystemResource(JsonFile);
		TableLayoutDesc layout = KijiTableLayout
				.readTableLayoutDescFromJSON(jsonStream);
		return layout;
	}

	public static void main(String[] args) throws Exception {
		KijiURI kijiURI = KijiURI.newBuilder().withInstanceName("default")
				.build();
		Kiji kiji = Kiji.Factory.open(kijiURI);
		KijiTable table = kiji.openTable("paymentAll");
		KijiTableReader reader = table.openTableReader();
		final KijiDataRequest dataRequest = KijiDataRequest.builder()
				.addColumns(ColumnsDef.create().add("family", "qualifier"))
				.build();
		final KijiScannerOptions scanOptions = new KijiScannerOptions()
				.setStartRow(table.getEntityId("the-start-row")).setStopRow(
						table.getEntityId("the-stop-row"));

		Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
		DecodedCell<Integer> value1 = new DecodedCell<Integer>(SCHEMA_INT,
				2241244);
		ColumnValueEqualsRowFilter equalFilter1 = new ColumnValueEqualsRowFilter(
				"PaymentViewStatus", "Customer_Id", value1);
		// ColumnValueEqualsRowFilter equalFilter2 = new
		// ColumnValueEqualsRowFilter("PaymentViewStatus", "Customer_Id",
		// value);
		AndRowFilter andFilter = new AndRowFilter(equalFilter1);
		scanOptions.setKijiRowFilter(andFilter);

		final KijiRowScanner scanner = reader.getScanner(dataRequest,
				scanOptions);

	}
}
