package org.bson;

import oracle.jdbc.driver.json.binary.OsonGeneratorImpl;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import org.bson.types.Decimal128;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class MyBSONToOSONConverter {
	private final OracleJsonFactory factory = getFactoryFromCache();

	private static OracleJsonFactory getFactoryFromCache() {
		OracleJsonFactory result;
		if ((result = factoryCache.get(Thread.currentThread().threadId())) == null) {
			result = new OracleJsonFactory();
			factoryCache.put(Thread.currentThread().threadId(), result);
		}

		return result;
	}

	private final MyByteArrayOutputStream out = new MyByteArrayOutputStream();
	private OracleJsonGenerator gen;
	final static Map<Long, OracleJsonFactory> factoryCache = new HashMap<>();
	private final boolean relativeOffsets;
	private final boolean lastValueSharing;
	private final boolean simpleValueSharing;
	private boolean allowDuplicateKeys;
	protected int bsonLength;
	protected String oid;

	public MyBSONToOSONConverter() {
		this.relativeOffsets = this.lastValueSharing = this.simpleValueSharing = false;
	}

	public MyBSONToOSONConverter(boolean allowDuplicateKeys, boolean relativeOffsets, boolean lastValueSharing, boolean simpleValueSharing) {
		this.allowDuplicateKeys = allowDuplicateKeys;
		this.relativeOffsets = relativeOffsets;
		this.lastValueSharing = lastValueSharing;
		this.simpleValueSharing = simpleValueSharing;
	}

	public static AtomicLong KEYS_SIZE = new AtomicLong(0);

	public long getKeysSize() {
		return keysSize;
	}

	private long keysSize;

	public final String getOid() {
		return oid;
	}

	public final boolean hasOid() {
		return oid != null;
	}

	public final byte[] getOSONData() {
		return out.toByteArray();
	}

	public int getBsonLength() {
		return bsonLength;
	}

	public void reset() {
		// reset OSON generation
		oid = null;
		out.reset();
		gen = factory.createJsonBinaryGenerator(out);
		final OsonGeneratorImpl _gen = (OsonGeneratorImpl) gen;
		_gen.setDuplicateKeyMode(allowDuplicateKeys ? OsonGeneratorImpl.DuplicateKeyMode.ALLOW : OsonGeneratorImpl.DuplicateKeyMode.DISALLOW);
		_gen.setUseRelativeOffsets(relativeOffsets);
		_gen.setLastValueSharing(lastValueSharing);
		_gen.setSimpleValueSharing(simpleValueSharing);
		/*			((OsonGeneratorImpl) gen).setTinyNodeStat(true);
		 */
	}

	public void convertBSONToOSON(final RawBsonDocument doc) {
		reset();

		final ByteBuffer buf = doc.getByteBuffer().asNIO();
		this.bsonLength = buf.getInt();
		//System.out.println("Root, size=" + bsonLength);
		final int maxSize = buf.position() + bsonLength;

		try {
			gen.writeStartObject();

			while (buf.position() < maxSize) {
				final byte type = buf.get();

				switch (type) {
					case 0:
						gen.writeEnd();
						gen.close();
						return;
					case 1:
						readDoubleField(buf/*, 1*/);
						break;
					case 2:
						readStringField(buf/*, 1*/);
						break;
					case 3:
						readDocument(buf/*, 1*/);
						break;
					case 4:
						readArray(buf/*, 1*/);
						break;
					case 7:
						readObjectIdField(buf/*, 1*/);
						break;
					case 8:
						readBooleanField(buf/*, level + 1*/);
						break;
					case 9:
						readUTCDatetimeField(buf/*, 1*/);
						break;
					case 10:
						readNullField(buf/*, 1*/);
						break;
					case 16:
						readIntField(buf/*, 1*/);
						break;
					case 18:
						readLongField(buf/*, 1*/);
						break;
					case 19:
						readDecimal128Field(buf/*, level + 1*/);
						break;
					default:
						System.out.println("/!\\ type not managed yet in root: " + type);
				}
			}
		}
		catch(BufferUnderflowException bue) {
			throw new RuntimeException("Error converting BSON to OSON:\n"+doc.toJson(), bue);
		}
	}

	private void readUTCDatetimeField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "UTC Datetime field");
		final String fieldName = readCString(buf);
		keysSize+=fieldName.length();
		final long value = buf.getLong();
		gen.write(fieldName, Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
	}

	private void readUTCDatetimeScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "UTC Datetime field");
		final String fieldName = readCString(buf);
		keysSize+=fieldName.length();
		final long value = buf.getLong();
		gen.write(Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + Instant.ofEpochMilli(value).atOffset(ZoneOffset.UTC));
	}

	private void readArray(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Array");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		gen.writeStartArray(fieldName);

		final int size = buf.getInt();
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size);

		final int maxSize = buf.position() + size;

		while (buf.position() < maxSize) {
			final byte type = buf.get();

			switch (type) {
				case 0:
					gen.writeEnd();
					return;
				case 1:
					readDoubleScalar(buf/*, level + 1*/);
					break;
				case 2:
					readStringScalar(buf/*, level + 1*/);
					break;
				case 16:
					readIntScalar(buf/*, level + 1*/);
					break;
				case 19:
					readDecimal128Scalar(buf/*, level + 1*/);
					break;
				case 18:
					readLongScalar(buf/*, level + 1*/);
					break;
				case 7:
					readObjectIdScalar(buf/*, level + 1*/);
					break;
				case 3:
					readDocumentInArray(buf/*, level + 1*/);
					break;
				case 4:
					readArrayInArray(buf/*, level + 1*/);
					break;
				case 8:
					readBooleanScalar(buf/*, level + 1*/);
					break;
				case 9:
					readUTCDatetimeScalar(buf/*, 1*/);
					break;
				default:
					System.out.println("/!\\ type not managed yet in array: " + type);
			}
		}
	}

	private void readArrayInArray(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Array");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		gen.writeStartArray();

		final int size = buf.getInt();
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size);

		final int maxSize = buf.position() + size;

		while (buf.position() < maxSize) {
			final byte type = buf.get();

			switch (type) {
				case 0:
					gen.writeEnd();
					return;
				case 1:
					readDoubleScalar(buf/*, level + 1*/);
					break;
				case 2:
					readStringScalar(buf/*, level + 1*/);
					break;
				case 16:
					readIntScalar(buf/*, level + 1*/);
					break;
				case 19:
					readDecimal128Scalar(buf/*, level + 1*/);
					break;
				case 18:
					readLongScalar(buf/*, level + 1*/);
					break;
				case 7:
					readObjectIdScalar(buf/*, level + 1*/);
					break;
				case 3:
					readDocumentInArray(buf/*, level + 1*/);
					break;
				case 4:
					readArrayInArray(buf/*, level + 1*/);
					break;
				case 8:
					readBooleanScalar(buf/*, level + 1*/);
					break;
				case 9:
					readUTCDatetimeScalar(buf/*, 1*/);
					break;
				default:
					System.out.println("/!\\ type not managed yet in array: " + type);
			}
		}
	}

	private void readStringScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "String scalar");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		final int size = buf.getInt();

		if (size == 2) {
			final byte asciiByte = buf.get();
			buf.get();
			gen.write(asciiByte < 0 ? REPLACEMENT_STRING : ONE_BYTE_ASCII_STRINGS[asciiByte]);
		}
		else {
			buf.get(readStringBuffer, 0, size);
			//this.buffer.get(); // read null
			gen.write(new String(readStringBuffer, 0, size - 1, UTF8_CHARSET));
		}

		//System.out.println(" ".repeat(level * 4) + "Value: " + value);
	}

	private void readDocument(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Document");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		//System.out.println("Document "+fieldName+" "+inArray);

		gen.writeStartObject(fieldName);

		final int size = buf.getInt();
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size);

		final int maxSize = buf.position() + size;

		while (buf.position() < maxSize) {
			final byte type = buf.get();

			switch (type) {
				case 0:
					gen.writeEnd();
					return;
				case 1:
					readDoubleField(buf/*, level + 1*/);
					break;
				case 2:
					readStringField(buf/*, level + 1*/);
					break;
				case 10:
					readNullField(buf/*, level + 1*/);
					break;
				case 16:
					readIntField(buf/*, level + 1*/);
					break;
				case 18:
					readLongField(buf/*, level + 1*/);
					break;
				case 3:
					readDocument(buf/*, level + 1*/);
					break;
				case 4:
					readArray(buf/*, level + 1*/);
					break;
				case 8:
					readBooleanField(buf/*, level + 1*/);
					break;
				case 19:
					readDecimal128Field(buf/*, level + 1*/);
					break;
				case 9:
					readUTCDatetimeField(buf/*, 1*/);
					break;
				case 7:
					readObjectIdField(buf/*, 1*/);
					break;
				default:
					System.out.println("/!\\ type not managed yet in document: " + type);
			}
		}

		//System.out.println(" ".repeat(level * 4) + buf.position() + "/" + maxSize + " since size=" + size);
	}
	private void readDocumentInArray(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Document");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		//System.out.println("Document "+fieldName+" "+inArray);

		gen.writeStartObject();

		final int size = buf.getInt();
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size);

		final int maxSize = buf.position() + size;

		while (buf.position() < maxSize) {
			final byte type = buf.get();

			switch (type) {
				case 0:
					gen.writeEnd();
					return;
				case 1:
					readDoubleField(buf/*, level + 1*/);
					break;
				case 2:
					readStringField(buf/*, level + 1*/);
					break;
				case 10:
					readNullField(buf/*, level + 1*/);
					break;
				case 16:
					readIntField(buf/*, level + 1*/);
					break;
				case 18:
					readLongField(buf/*, level + 1*/);
					break;
				case 3:
					readDocument(buf/*, level + 1*/);
					break;
				case 4:
					readArray(buf/*, level + 1*/);
					break;
				case 8:
					readBooleanField(buf/*, level + 1*/);
					break;
				case 19:
					readDecimal128Field(buf/*, level + 1*/);
					break;
				case 9:
					readUTCDatetimeField(buf/*, 1*/);
					break;
				case 7:
					readObjectIdField(buf/*, 1*/);
					break;
				default:
					System.out.println("/!\\ type not managed yet in document in array: " + type);
			}
		}

		//System.out.println(" ".repeat(level * 4) + buf.position() + "/" + maxSize + " since size=" + size);
	}

	private void readNullField(final ByteBuffer buf/*, final int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Null field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		gen.writeNull(fieldName);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: null");
	}

	private void readIntField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Int field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final int value = buf.getInt();
		gen.write(fieldName, value);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}
	private void readBooleanField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Int field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final boolean value = buf.get() == 1;
		gen.write(fieldName, value);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}
	private void readBooleanScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Int field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final boolean value = buf.get() == 1;
		gen.write(value);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}

	private void readLongField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Long field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final long value = buf.getLong();
		gen.write(fieldName, value);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}

	private void readDoubleField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Double field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final double value = buf.getDouble();
		gen.write(fieldName, value);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}
	private void readDecimal128Field(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Double field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final long low = buf.getLong();
		final long high = buf.getLong();

		final Decimal128 value = Decimal128.fromIEEE754BIDEncoding(high, low);
		gen.write(fieldName, value.bigDecimalValue());
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}
	private void readDecimal128Scalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Double field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final long low = buf.getLong();
		final long high = buf.getLong();

		final Decimal128 value = Decimal128.fromIEEE754BIDEncoding(high, low);
		gen.write(value.bigDecimalValue());
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", value: " + value);
	}

	private void readIntScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Int scalar");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		final int value = buf.getInt();
		gen.write(value);
		//System.out.println(" ".repeat(level * 4) + "Value: " + value);
	}

	private void readLongScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Long scalar");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();
		final long value = buf.getLong();
		gen.write(value);
		//System.out.println(" ".repeat(level * 4) + "Value: " + value);
	}

	private void readDoubleScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "Double scalar");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		final double value = buf.getDouble();
		gen.write(value);
		//System.out.println(" ".repeat(level * 4) + "Value: " + value);
	}

	private void readStringField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "String field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		final int size = buf.getInt();

		if (size == 2) {
			final byte asciiByte = buf.get();
			buf.get();
			gen.write(fieldName, asciiByte < 0 ? REPLACEMENT_STRING : ONE_BYTE_ASCII_STRINGS[asciiByte]);
		}
		else {
			buf.get(readStringBuffer, 0, size);
			//this.buffer.get(); // read null
			gen.write(fieldName, new String(readStringBuffer, 0, size - 1, UTF8_CHARSET));
		}
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size + ", value: " + value);
	}

	private static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	public static String toHexString(final byte[] bytes) {
		final char[] chars = new char[24];
		for (int i = 0, j = 0; j < 12; ++j) {
			final byte b = bytes[j];
			chars[i++] = HEX_CHARS[b >> 4 & 15];
			chars[i++] = HEX_CHARS[b & 15];
		}

		return new String(chars, 0, 24);
	}

	private final byte[] tempOid = new byte[12];
	private void readObjectIdField(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "String field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		buf.get(tempOid,0,12);

		if (oid == null && "_id".equals(fieldName)) {
			oid = toHexString(tempOid);
		}

		gen.writeKey(fieldName);
		gen.writeId(tempOid);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size + ", value: " + value);
	}

	private void readObjectIdScalar(final ByteBuffer buf/*, int level*/) {
		//System.out.println(" ".repeat(level * 4) + "String field");
		final String fieldName = readCString(buf);
		keysSize += fieldName.length();

		buf.get(tempOid,0,12);

		gen.writeId(tempOid);
		//System.out.println(" ".repeat(level * 4) + "Field: " + fieldName + ", size: " + size + ", value: " + value);
	}

	private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
	private static final String REPLACEMENT_STRING = UTF8_CHARSET.newDecoder().replacement();
	private static final String[] ONE_BYTE_ASCII_STRINGS = new String[128];

	static {
		for (int b = 0; b < ONE_BYTE_ASCII_STRINGS.length; ++b) {
			ONE_BYTE_ASCII_STRINGS[b] = String.valueOf((char) b);
		}
	}

	private final byte[] readStringBuffer = new byte[1024 * 1024];

	private String readCString(final ByteBuffer buf) {
		final int mark = buf.position();
		skipCString(buf);
		final int size = buf.position() - mark;
		buf.position(mark);

		if (size == 2) {
			final byte asciiByte = buf.get();
			buf.get();
			return asciiByte < 0 ? REPLACEMENT_STRING : ONE_BYTE_ASCII_STRINGS[asciiByte];
		}
		else {
			buf.get(readStringBuffer, 0, size);
			//this.buffer.get(); // read null
			return new String(readStringBuffer, 0, size - 1, UTF8_CHARSET);
		}
	}

	private final void skipCString(final ByteBuffer buf) {
		final int rem = buf.remaining();
		for (int i = 0; i < rem; i++) {
			if (buf.get() == 0) return;
		}

		throw new BsonSerializationException("Found a BSON string that is not null-terminated");
	}

	public static void main(String[] args) throws Throwable {
/*
		Document useIdIndexHint = new Document("_id", 1);

		final MongoClientSettings settings =
				MongoClientSettings.builder()
						.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
						.credential(MongoCredential.createCredential("merndemo", "merndemo", "Str0ng_Password".toCharArray()))
						.applyToConnectionPoolSettings(builder ->
								builder.maxSize(10).minSize(10).maxConnecting(10).maxWaitTime(1,TimeUnit.DAYS).maxConnectionIdleTime(10, TimeUnit.MINUTES))
						.applyToClusterSettings(builder ->
								builder.hosts(Arrays.asList(new ServerAddress("localhost", 27016))))
						.build();

		try (MongoClient mongoClient = MongoClients.create(settings)) {
			MongoDatabase mongoDatabase = mongoClient.getDatabase("merndemo");
			MongoCollection<RawBsonDocument> collection = mongoDatabase.getCollection("orders", RawBsonDocument.class);

			try (MongoCursor<RawBsonDocument> cursor = collection.find().hint(useIdIndexHint).batchSize(100).cursor()) {
				while (cursor.hasNext()) {
					final RawBsonDocument doc = cursor.next();


					System.out.println(doc.toJson());
				}
			}
		}
*/
		String j = Files.readString(new File("test2.json").toPath());

		MyBSONToOSONConverter dec = new MyBSONToOSONConverter(true, true, true, true);
		final RawBsonDocument raw = RawBsonDocument.parse(j);

		FileOutputStream o = new FileOutputStream("test2.bson.out");
		ByteBuffer buf = RawBsonDocument.parse(j).getByteBuffer().asNIO();
		byte[] bson = new byte[buf.remaining()];
		buf.get(bson, 0, bson.length);
		o.write(bson);
		o.close();

		/*for(int i = 0; i < 20000; i++) {
			dec.convertBSONToOSON(raw);
		}*/

		long start = System.nanoTime();
		dec.convertBSONToOSON(raw);
		long end = System.nanoTime();
		System.out.println("New duration="+(end-start));

		o = new FileOutputStream("test2.oson.out");
		o.write(dec.getOSONData());
		o.close();

		System.out.println(dec.getOid()+", "+dec.getKeysSize()+", "+dec.getBsonLength()+", "+dec.getOSONData());

	}
}
