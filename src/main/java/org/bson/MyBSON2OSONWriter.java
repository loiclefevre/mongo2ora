package org.bson;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class MyBSON2OSONWriter implements BsonWriter {
	private static final OracleJsonFactory factory = new OracleJsonFactory();
	private final ByteArrayOutputStream out = new ByteArrayOutputStream();
	//private JsonGenerator gen;
	private OracleJsonGenerator gen;

	private State state;
	private Context context;
	private int serializationDepth;

	private String oid;

	public MyBSON2OSONWriter() {
		state = State.INITIAL;
	}

	protected final String getName() {
		return context.getContextType() == BsonContextType.ARRAY ? Integer.toString(context.index++) : context.name;
	}

	protected Context getContext() {
		return context;
	}

	@Override
	public void flush() {
	}

	@Override
	public void writeBinaryData(String name, BsonBinary binary) {
		this.writeName(name);
		this.writeBinaryData(binary);
	}

	@Override
	public void writeBinaryData(BsonBinary binary) {
		if (binary.getType() == BsonBinarySubType.UUID_LEGACY.getValue()) {
			final UUID uuid = new UUID(Bits.readLong(binary.getData(), 0), Bits.readLong(binary.getData(), 8));
			if (context.getContextType() == BsonContextType.ARRAY) {
				gen.write(uuid.toString());
			} else {
				gen.write(this.getName(), uuid.toString());
			}
		} else {
			if (context.getContextType() == BsonContextType.ARRAY) {
				gen.write(hexa(binary.getData()));
			} else {
				gen.write(this.getName(), hexa(binary.getData()));
			}
		}
		state = getNextState();
	}

	@Override
	public void writeBoolean(String name, boolean value) {
		this.writeName(name);
		this.writeBoolean(value);
	}

	@Override
	public void writeBoolean(boolean value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeDateTime(String name, long value) {
		this.writeName(name);
		this.writeDateTime(value);
	}

	@Override
	public void writeDateTime(long value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
		} else {
			gen.write(this.getName(), Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
		}
		state = getNextState();
	}

	@Override
	public void writeDBPointer(String s, BsonDbPointer bsonDbPointer) {
		throw new UnsupportedOperationException("DBRef");
	}

	@Override
	public void writeDBPointer(BsonDbPointer bsonDbPointer) {
		throw new UnsupportedOperationException("DBRef");
	}

	@Override
	public void writeDouble(String name, double value) {
		this.writeName(name);
		this.writeDouble(value);
	}

	@Override
	public void writeDouble(double value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeEndArray() {
		--this.serializationDepth;
		context = context.getParentContext();
		gen.writeEnd();
		// document is a top level array
		if (this.serializationDepth == 0) {
			gen.close();
		}
		state = getNextState();
	}

	/**
	 * OK
	 */
	@Override
	public void writeEndDocument() {
		--this.serializationDepth;

		context = context.getParentContext();

		gen.writeEnd();

		// gen.close ?
		if (this.serializationDepth == 0) {
			gen.close();
		}

		if (context != null && context.getContextType() != BsonContextType.TOP_LEVEL) {
			state = getNextState();
		} else {
			state = State.DONE;
		}
	}

	@Override
	public void writeInt32(String name, int value) {
		this.writeName(name);
		this.writeInt32(value);
	}

	@Override
	public void writeInt32(int value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeInt64(String name, long value) {
		this.writeName(name);
		this.writeInt64(value);
	}

	@Override
	public void writeInt64(long value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeDecimal128(String name, Decimal128 value) {
		this.writeName(name);
		this.writeDecimal128(value);
	}

	@Override
	public void writeDecimal128(Decimal128 value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value.bigDecimalValue());
		} else {
			gen.write(this.getName(), value.bigDecimalValue());
		}
		state = getNextState();
	}

	@Override
	public void writeJavaScript(String s) {
		throw new UnsupportedOperationException("Code");
	}

	@Override
	public void writeJavaScript(String s, String s1) {
		throw new UnsupportedOperationException("Code");
	}

	@Override
	public void writeJavaScriptWithScope(String s) {
		throw new UnsupportedOperationException("CodeWScope");
	}

	@Override
	public void writeJavaScriptWithScope(String s, String s1) {
		throw new UnsupportedOperationException("CodeWScope");
	}

	@Override
	public void writeMaxKey() {
		throw new UnsupportedOperationException("MaxKey");
	}

	@Override
	public void writeMaxKey(String s) {
		throw new UnsupportedOperationException("MaxKey");
	}

	@Override
	public void writeMinKey() {
		throw new UnsupportedOperationException("MinKey");
	}

	@Override
	public void writeMinKey(String s) {
		throw new UnsupportedOperationException("MinKey");
	}

	@Override
	public void writeNull(String name) {
		this.writeName(name);
		this.writeNull();
	}

	@Override
	public void writeNull() {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.writeNull();
		} else {
			gen.writeNull(this.getName());
		}
		state = getNextState();
	}

	@Override
	public void writeObjectId(String name, ObjectId objectId) {
		this.writeName(name);
		this.writeObjectId(objectId);
	}

	private static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	public static String toHexString(final byte[] bytes) {
		final char[] chars = new char[24];
		for(int i=0,j = 0; j < 12; ++j) {
			final byte b = bytes[j];
			chars[i++] = HEX_CHARS[b >> 4 & 15];
			chars[i++] = HEX_CHARS[b & 15];
		}

		return new String(chars,0,24);
	}


	@Override
	public void writeObjectId(final ObjectId objectId) {
		final String _id = objectId.toString();

		if (oid == null && "_id".equals(this.getName())) {
			oid = _id;
		}

		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(_id);
		} else {
			gen.write(this.getName(), _id);
			//gen.write("test_buffer", "XDLKFQMLQKSDMFLKQSDMLFKQSMDLFKQSMDLFKMQSLDFKMLQSDFKMIPQOQSFKIDIPDIFDIFPFIDISFDSPQSDOFILQSDKFLQKSDFAA");
		}
		state = getNextState();
	}


	public void writeObjectId2(final byte[] objectId) {

		final String _id = toHexString(objectId);

		if (oid == null && "_id".equals(this.getName())) {
			oid = _id;
		}

		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(_id);
		} else {
			gen.write(this.getName(), _id);
			//gen.write("test_buffer", "XDLKFQMLQKSDMFLKQSDMLFKQSMDLFKQSMDLFKMQSLDFKMLQSDFKMIPQOQSFKIDIPDIFDIFPFIDISFDSPQSDOFILQSDKFLQKSDFAA");
		}
		state = getNextState();
	}

	@Override
	public void writeRegularExpression(BsonRegularExpression bsonRegularExpression) {
		throw new UnsupportedOperationException("Regex");
	}

	@Override
	public void writeRegularExpression(String s, BsonRegularExpression bsonRegularExpression) {
		throw new UnsupportedOperationException("Regex");
	}

	@Override
	public void writeStartArray(String name) {
		this.writeName(name);
		this.writeStartArray();
	}

	@Override
	public void writeStartArray() {
		++this.serializationDepth;
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.writeStartArray();
		} else {
			gen.writeStartArray(this.getName());
		}
		context = new Context(context, BsonContextType.ARRAY);
		state = State.VALUE;
	}


	@Override
	public void writeStartDocument(String name) {
		this.writeName(name);
		this.writeStartDocument();
	}

	/**
	 * OK
	 */
	@Override
	public void writeStartDocument() {
		++this.serializationDepth;

		BsonContextType contextType = state == State.SCOPE_DOCUMENT ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
		if (context != null && contextType != BsonContextType.SCOPE_DOCUMENT) {
			if (context.getContextType() == BsonContextType.ARRAY) {
				gen.writeStartObject();
			} else {
				gen.writeStartObject(this.getName());
			}
		} else {
			gen.writeStartObject();
		}

		context = new Context(context, contextType);
		state = State.NAME;
	}


	@Override
	public void writeString(String name, String value) {
		this.writeName(name);
		this.writeString(value);
	}

	@Override
	public void writeString(String value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeSymbol(String name, String value) {
		this.writeName(name);
		this.writeSymbol(value);
	}

	@Override
	public void writeSymbol(String value) {
		if (context.getContextType() == BsonContextType.ARRAY) {
			gen.write(value);
		} else {
			gen.write(this.getName(), value);
		}
		state = getNextState();
	}

	@Override
	public void writeTimestamp(BsonTimestamp bsonTimestamp) {
		throw new UnsupportedOperationException("Timestamp");
	}

	@Override
	public void writeTimestamp(String s, BsonTimestamp bsonTimestamp) {
		throw new UnsupportedOperationException("Timestamp");
	}

	@Override
	public void writeUndefined() {
		throw new UnsupportedOperationException("Undefined");
	}

	@Override
	public void writeUndefined(String s) {
		throw new UnsupportedOperationException("Undefined");
	}

	@Override
	public final void pipe(BsonReader reader) {
	}

	public final void pipe(MyBSONReader reader) {
		pipeDocument(reader);
	}

	private void pipeDocument(MyBSONReader reader) {
		reader.readStartDocument();
		this.writeStartDocument();

		do {
			if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
				reader.readEndDocument();

				this.writeEndDocument();
				return;
			}

			// save current field name inside context
			this.writeName(reader.readName());

			this.pipeValue(reader);
		} while (true);
	}

	private void pipeDocument(BsonDocument value) {
		this.writeStartDocument();
		Iterator var2 = value.entrySet().iterator();

		while (var2.hasNext()) {
			Map.Entry<String, BsonValue> cur = (Map.Entry) var2.next();
			this.writeName(cur.getKey());
			this.pipeValue((BsonValue) cur.getValue());
		}

		this.writeEndDocument();
	}

	private void pipeArray(MyBSONReader reader) {
		reader.readStartArray();
		this.writeStartArray();

		do {
			if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
				reader.readEndArray();
				this.writeEndArray();
				return;
			}

			this.pipeValue(reader);
		} while (true);

	}

	private void pipeArray(BsonArray array) {
		this.writeStartArray();
		Iterator var2 = array.iterator();

		while (var2.hasNext()) {
			BsonValue cur = (BsonValue) var2.next();
			this.pipeValue(cur);
		}

		this.writeEndArray();
	}

	private void pipeJavascriptWithScope(BsonJavaScriptWithScope javaScriptWithScope) {
		this.writeJavaScriptWithScope(javaScriptWithScope.getCode());
		this.pipeDocument(javaScriptWithScope.getScope());
	}

	private void pipeJavascriptWithScope(MyBSONReader reader) {
		this.writeJavaScriptWithScope(reader.readJavaScriptWithScope());
		this.pipeDocument(reader);
	}

	private void pipeValue(MyBSONReader reader) {
		switch (reader.getCurrentBsonType()) {
			case DOCUMENT:
				this.pipeDocument(reader);
				break;
			case ARRAY:
				this.pipeArray(reader);
				break;
			case DOUBLE:
				this.writeDouble(reader.readDouble());
				break;
			case STRING:
				this.writeString(reader.readString());
				break;
			case BINARY:
				this.writeBinaryData(reader.readBinaryData());
				break;
			case UNDEFINED:
				reader.readUndefined();
				this.writeUndefined();
				break;
			case OBJECT_ID:
				this.writeObjectId2(reader.readObjectId2());
				break;
			case BOOLEAN:
				this.writeBoolean(reader.readBoolean());
				break;
			case DATE_TIME:
				this.writeDateTime(reader.readDateTime());
				break;
			case NULL:
				reader.readNull();
				this.writeNull();
				break;
			case REGULAR_EXPRESSION: // not supported
				this.writeRegularExpression(reader.readRegularExpression());
				break;
			case JAVASCRIPT: // not supported
				this.writeJavaScript(reader.readJavaScript());
				break;
			case SYMBOL:
				this.writeSymbol(reader.readSymbol());
				break;
			case JAVASCRIPT_WITH_SCOPE: // not supported
				this.pipeJavascriptWithScope(reader);
				break;
			case INT32:
				this.writeInt32(reader.readInt32());
				break;
			case TIMESTAMP: // not supported
				this.writeTimestamp(reader.readTimestamp());
				break;
			case INT64:
				this.writeInt64(reader.readInt64());
				break;
			case DECIMAL128:
				this.writeDecimal128(reader.readDecimal128());
				break;
			case MIN_KEY: // not supported
				reader.readMinKey();
				this.writeMinKey();
				break;
			case DB_POINTER: // not supported
				this.writeDBPointer(reader.readDBPointer());
				break;
			case MAX_KEY: // not supported
				reader.readMaxKey();
				this.writeMaxKey();
				break;
			default:
				throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBsonType());
		}
	}

	private void pipeValue(BsonValue value) {
		switch (value.getBsonType()) {
			case DOCUMENT:
				this.pipeDocument(value.asDocument());
				break;
			case ARRAY:
				this.pipeArray(value.asArray());
				break;
			case DOUBLE:
				this.writeDouble(value.asDouble().getValue());
				break;
			case STRING:
				this.writeString(value.asString().getValue());
				break;
			case BINARY:
				this.writeBinaryData(value.asBinary());
				break;
			case UNDEFINED:
				this.writeUndefined();
				break;
			case OBJECT_ID:
				this.writeObjectId(value.asObjectId().getValue());
				break;
			case BOOLEAN:
				this.writeBoolean(value.asBoolean().getValue());
				break;
			case DATE_TIME:
				this.writeDateTime(value.asDateTime().getValue());
				break;
			case NULL:
				this.writeNull();
				break;
			case REGULAR_EXPRESSION:
				this.writeRegularExpression(value.asRegularExpression());
				break;
			case JAVASCRIPT:
				this.writeJavaScript(value.asJavaScript().getCode());
				break;
			case SYMBOL:
				this.writeSymbol(value.asSymbol().getSymbol());
				break;
			case JAVASCRIPT_WITH_SCOPE:
				this.pipeJavascriptWithScope(value.asJavaScriptWithScope());
				break;
			case INT32:
				this.writeInt32(value.asInt32().getValue());
				break;
			case TIMESTAMP:
				this.writeTimestamp(value.asTimestamp());
				break;
			case INT64:
				this.writeInt64(value.asInt64().getValue());
				break;
			case DECIMAL128:
				this.writeDecimal128(value.asDecimal128().getValue());
				break;
			case MIN_KEY:
				this.writeMinKey();
				break;
			case DB_POINTER:
				this.writeDBPointer(value.asDBPointer());
				break;
			case MAX_KEY:
				this.writeMaxKey();
				break;
			default:
				throw new IllegalArgumentException("unhandled BSON type: " + value.getBsonType());
		}
	}

	public void reset() {
		reset(true);
	}

	public void reset(boolean outputOsonFormat) {
		// reset OSON generation
		oid = null;
		out.reset();
		gen = outputOsonFormat ? factory.createJsonBinaryGenerator(out) : factory.createJsonTextGenerator(out);
		//gen = ogen.wrap(JsonGenerator.class);
		state = State.INITIAL;
		context = null;
		this.serializationDepth = 0;
	}

	protected State getNextState() {
		return context.getContextType() == BsonContextType.ARRAY ? State.VALUE : State.NAME;
	}

	public void writeName(String name) {
		if (state != State.NAME) {
			throw new IllegalStateException("WriteName");
		}

		context.name = name;
		state = State.VALUE;
	}

	public final String getOid() {
		return oid;
	}

	public final byte[] getOSONBytes() {
		return out.toByteArray();
	}

	public class Context {
		private final Context parentContext;
		private final BsonContextType contextType;

		private int index;
		private String code;
		private String name;

		Context(Context parentContext, BsonContextType contextType) {
			this.parentContext = parentContext;
			this.contextType = contextType;
		}

		public Context(Context from) {
			this.parentContext = from.parentContext;
			this.contextType = from.contextType;
		}

		public Context getParentContext() {
			return this.parentContext;
		}

		public BsonContextType getContextType() {
			return this.contextType;
		}

		public Context copy() {
			return new Context(this);
		}
	}

	private static String hexa(final byte[] data) {
		final char[] result = new char[data.length * 2];

		int j = 0;
		for (int i = 0; i < data.length; i++) {
			final int x = data[i];
			int k = (x >> 4) & 0xF;
			result[j++] = (char) (k < 10 ? '0' + k : 'A' + k - 10);
			k = x & 0xF;
			result[j++] = (char) (k < 10 ? '0' + k : 'A' + k - 10);
		}

		return new String(result);
	}

	public enum State {
		INITIAL,
		NAME,
		VALUE,
		SCOPE_DOCUMENT,
		DONE,
		CLOSED;
	}
}
