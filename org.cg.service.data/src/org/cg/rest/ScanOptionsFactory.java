package org.cg.rest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.avro.Schema;
import org.cg.filters.ColumnValueGreaterEqualsRowFilter;
import org.cg.representation.Request;
import org.kiji.rest.KijiClient;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.filter.AndRowFilter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

public class ScanOptionsFactory {

	public static KijiScannerOptions buildScanOptions(KijiClient kijiClient,
			String instance, String table, Request request) {
		final KijiScannerOptions scanOptions = new KijiScannerOptions();
		String startCompKey = request.getStartCompKey();
		String stopCompKey = request.getEndCompKey();
		// Open Kiji Table, get table layout
		KijiTable kijiTable = kijiClient.getKijiTable(instance, table);
		KijiTableLayout layout = kijiTable.getLayout();
		TableLayoutDesc desc = layout.getDesc();
		String keyFormat = desc.getKeysFormat().toString();
		// Calculate number of components in key
		int componentsNum = getComponentsNum(keyFormat, "components");
		String[] types = null;
		String keyTypes = request.getKeyTypes();
		// Check if key types input is correct
		if (keyTypes != null) {
			types = keyTypes.split(",");
			if (componentsNum != types.length) {
				throw new WebApplicationException(new IllegalArgumentException(
						"Ambiguous request. "
								+ "Incorrect number of key types input."),
						Status.BAD_REQUEST);
			}
		} else if (startCompKey != null || stopCompKey != null) {
			throw new WebApplicationException(new IllegalArgumentException(
					"Ambiguous request. " + "Lack of key types input."),
					Status.BAD_REQUEST);
		}
		try {
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
				scanOptions.setStopRow(stopRowId);
			}

			Schema SCHEMA_INT = Schema.create(Schema.Type.INT);
			DecodedCell<Integer> value1 = new DecodedCell<Integer>(SCHEMA_INT,
					2241244);
			ColumnValueGreaterEqualsRowFilter equalFilter1 = new ColumnValueGreaterEqualsRowFilter(
					"PaymentViewStatus", "Customer_Id", value1);
			// ColumnValueEqualsRowFilter equalFilter2 = new ColumnValueEqualsRowFilter("PaymentViewStatus", "Customer_Id", value1);
			AndRowFilter andFilter = new AndRowFilter(equalFilter1);
			scanOptions.setKijiRowFilter(andFilter);
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(e, Status.BAD_REQUEST);
		} finally {
			ResourceUtils.releaseOrLog(kijiTable);
		}
		return scanOptions;
	}

	/**
	 * According target name to parse keyFormat, get total number of components
	 * 
	 * @param keyFormat
	 *            Key Format get from TableLayoutDesc
	 * @param target
	 *            Here is "components"
	 * @return number of components in key
	 */
	public static int getComponentsNum(String keyFormat, String target) {
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
	 * 
	 * @param key
	 *            input key string get from Query Parameter
	 * @param rowKey
	 *            Object array of output rowKey
	 * @param types
	 *            String array of data types
	 * @param index
	 */
	public static void setRowKey(String key, Object[] rowKey, String[] types, int index) {
		if (types[index].equals("String")) {
			rowKey[index] = key;
		} else if (types[index].equals("Long"))
			rowKey[index] = Long.parseLong(key);
		else if (types[index].equals("Integer"))
			rowKey[index] = Integer.parseInt(key);
	}

}
