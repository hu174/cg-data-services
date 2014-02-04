package org.cg.filters;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.filter.HasColumnDataRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterDeserializer;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.ToJson;

import com.google.common.base.Objects;

public class ColumnValueGreaterEqualsRowFilter extends KijiRowFilter {

	/** The name of the family node. */
	private static final String FAMILY_NODE = "family";

	/** The name of the qualifier node. */
	private static final String QUALIFIER_NODE = "qualifier";

	/** The name of the value node. */
	private static final String VALUE_NODE = "value";

	/** The name of the writer schema node. */
	private static final String SCHEMA_NODE = "writerSchema";

	/** The name of the data node. */
	private static final String DATA_NODE = "data";

	/** The name of the column family to check for data in. */
	private final String _Family;

	/** The name of the column qualifier to check for data in. */
	private final String _Qualifier;

	/** The value the most recent column value must equal to pass the filter. */
	private final DecodedCell<?> _Value;

	public ColumnValueGreaterEqualsRowFilter(String family, String qualifier,
			DecodedCell<?> value) {
		if (null == family || family.isEmpty()) {
			throw new IllegalArgumentException("family is required");
		}
		if (null == qualifier || qualifier.isEmpty()) {
			throw new IllegalArgumentException("qualifier is required");
		}
		if (null == value) {
			throw new IllegalArgumentException(
					"value may not be null. If you want to check for column data presence, use "
							+ HasColumnDataRowFilter.class.getName());
		}
		_Family = family;
		_Qualifier = qualifier;
		_Value = value;
	}

	@Override
	public KijiDataRequest getDataRequest() {
		// TODO Auto-generated method stub
		return KijiDataRequest.create(_Family, _Qualifier);
	}

	@Override
	protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
		// TODO Auto-generated method stub
		return ColumnValueGreaterEqualsRowFilterDeserializer.class;
	}

	@Override
	public Filter toHBaseFilter(Context context) throws IOException {
		// TODO Auto-generated method stub
		final KijiColumnName column = new KijiColumnName(_Family, _Qualifier);
		HBaseColumnName hbaseColumnName = context.getHBaseColumnName(column);
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(),
				CompareOp.GREATER, context.getHBaseCellValue(column,
						_Value));

		filter.setLatestVersionOnly(true);
		filter.setFilterIfMissing(true);

		// Skip the entire row if the filter does not allow the column value.
		return new SkipFilter(filter);
	}

	@Override
	protected JsonNode toJsonNode() {
		// TODO Auto-generated method stub
		final ObjectNode root = JsonNodeFactory.instance.objectNode();
		root.put(FAMILY_NODE, _Family);
		root.put(QUALIFIER_NODE, _Qualifier);
		final ObjectNode value = root.with(VALUE_NODE);
		// Schema's documentation for toString says it is rendered as JSON.
		value.put(SCHEMA_NODE, _Value.getWriterSchema().toString());
		try {
			value.put(
					DATA_NODE,
					ToJson.toAvroJsonString(_Value.getData(),
							_Value.getWriterSchema()));
		} catch (IOException ioe) {
			throw new KijiIOException(ioe);
		}
		return root;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ColumnValueGreaterEqualsRowFilter)) {
			return false;
		} else {
			final ColumnValueGreaterEqualsRowFilter otherFilter = (ColumnValueGreaterEqualsRowFilter) other;
			return Objects.equal(otherFilter._Family, this._Family)
					&& Objects.equal(otherFilter._Qualifier, this._Qualifier)
					&& Objects.equal(otherFilter._Value, this._Value);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(_Family, _Qualifier, _Value);
	}

	public static final class ColumnValueGreaterEqualsRowFilterDeserializer
			implements KijiRowFilterDeserializer {
		/** {@inheritDoc} */
		@Override
		public KijiRowFilter createFromJson(JsonNode root) {
			final String family = root.path(FAMILY_NODE).getTextValue();
			final String qualifier = root.path(QUALIFIER_NODE).getTextValue();
			final String schema = root.path(VALUE_NODE).path(SCHEMA_NODE)
					.getTextValue();
			final Schema writerSchema = (new Schema.Parser()).parse(schema);
			final String data = root.path(VALUE_NODE).path(DATA_NODE)
					.getTextValue();
			try {
				final DecodedCell<?> cell = new DecodedCell<Object>(
						writerSchema, FromJson.fromAvroJsonString(data,
								writerSchema));
				return new ColumnValueGreaterEqualsRowFilter(family, qualifier,
						cell);
			} catch (IOException ioe) {
				throw new KijiIOException(ioe);
			}
		}
	}
}
