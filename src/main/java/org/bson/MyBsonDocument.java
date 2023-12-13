package org.bson;

import org.bson.assertions.Assertions;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class MyBsonDocument extends BsonDocument {
	private static final long serialVersionUID = 1L;
	private static final int MIN_BSON_DOCUMENT_SIZE = 5;
	private final byte[] bytes;
	private final int offset;
	private final int length;

	public static MyBsonDocument parse(String json) {
		Assertions.notNull("json", json);
		return (new MyBsonDocumentCodec()).decode(new JsonReader(json), DecoderContext.builder().build());
	}

	public MyBsonDocument(byte[] bytes) {
		this((byte[]) Assertions.notNull("bytes", bytes), 0, bytes.length);
	}

	public MyBsonDocument(byte[] bytes, int offset, int length) {
		Assertions.notNull("bytes", bytes);
		Assertions.isTrueArgument("offset >= 0", offset >= 0);
		Assertions.isTrueArgument("offset < bytes.length", offset < bytes.length);
		Assertions.isTrueArgument("length <= bytes.length - offset", length <= bytes.length - offset);
		Assertions.isTrueArgument("length >= 5", length >= 5);
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
	}

	public <T> MyBsonDocument(T document, Codec<T> codec) {
		Assertions.notNull("document", document);
		Assertions.notNull("codec", codec);
		BasicOutputBuffer buffer = new BasicOutputBuffer();
		BsonBinaryWriter writer = new BsonBinaryWriter(buffer);

		try {
			codec.encode(writer, document, EncoderContext.builder().build());
			this.bytes = buffer.getInternalBuffer();
			this.offset = 0;
			this.length = buffer.getPosition();
		}
		finally {
			writer.close();
		}

	}

	public ByteBuf getByteBuffer() {
		ByteBuffer buffer = ByteBuffer.wrap(this.bytes, this.offset, this.length);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		return new ByteBufNIO(buffer);
	}

	public <T> T decode(Codec<T> codec) {
		return this.decode((Decoder<T>) codec);
	}

	public <T> T decode(Decoder<T> decoder) {
		BsonBinaryReader reader = this.createReader();

		T var3;
		try {
			var3 = decoder.decode(reader, DecoderContext.builder().build());
		}
		finally {
			reader.close();
		}

		return var3;
	}

	public void clear() {
		throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
	}

	public BsonValue put(String key, BsonValue value) {
		throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
	}

	public BsonDocument append(String key, BsonValue value) {
		throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
	}

	public void putAll(Map<? extends String, ? extends BsonValue> m) {
		throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
	}

	public BsonValue remove(Object key) {
		throw new UnsupportedOperationException("RawBsonDocument instances are immutable");
	}

	public boolean isEmpty() {
		BsonBinaryReader bsonReader = this.createReader();

		boolean var2;
		try {
			bsonReader.readStartDocument();
			if (bsonReader.readBsonType() == BsonType.END_OF_DOCUMENT) {
				bsonReader.readEndDocument();
				return true;
			}

			var2 = false;
		}
		finally {
			bsonReader.close();
		}

		return var2;
	}

	public int size() {
		int size = 0;
		BsonBinaryReader bsonReader = this.createReader();

		try {
			bsonReader.readStartDocument();

			while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
				++size;
				bsonReader.readName();
				bsonReader.skipValue();
			}

			bsonReader.readEndDocument();
			return size;
		}
		finally {
			bsonReader.close();
		}
	}

	public Set<Entry<String, BsonValue>> entrySet() {
		return this.toBaseBsonDocument().entrySet();
	}

	public Collection<BsonValue> values() {
		return this.toBaseBsonDocument().values();
	}

	public Set<String> keySet() {
		return this.toBaseBsonDocument().keySet();
	}

	public String getFirstKey() {
		BsonBinaryReader bsonReader = this.createReader();

		String var2;
		try {
			bsonReader.readStartDocument();

			try {
				var2 = bsonReader.readName();
			}
			catch (BsonInvalidOperationException var6) {
				throw new NoSuchElementException();
			}
		}
		finally {
			bsonReader.close();
		}

		return var2;
	}

	public boolean containsKey(Object key) {
		if (key == null) {
			throw new IllegalArgumentException("key can not be null");
		}
		else {
			BsonBinaryReader bsonReader = this.createReader();

			try {
				bsonReader.readStartDocument();

				while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
					if (bsonReader.readName().equals(key)) {
						boolean var3 = true;
						return var3;
					}

					bsonReader.skipValue();
				}

				bsonReader.readEndDocument();
				return false;
			}
			finally {
				bsonReader.close();
			}
		}
	}

	public boolean containsValue(Object value) {
		BsonBinaryReader bsonReader = this.createReader();

		try {
			bsonReader.readStartDocument();

			while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
				bsonReader.skipName();
				if (RawBsonValueHelper.decode(this.bytes, bsonReader).equals(value)) {
					boolean var3 = true;
					return var3;
				}
			}

			bsonReader.readEndDocument();
		}
		finally {
			bsonReader.close();
		}

		return false;
	}

	public BsonValue get(Object key) {
		Assertions.notNull("key", key);
		BsonBinaryReader bsonReader = this.createReader();

		try {
			bsonReader.readStartDocument();

			while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
				if (bsonReader.readName().equals(key)) {
					BsonValue var3 = RawBsonValueHelper.decode(this.bytes, bsonReader);
					return var3;
				}

				bsonReader.skipValue();
			}

			bsonReader.readEndDocument();
		}
		finally {
			bsonReader.close();
		}

		return null;
	}

	public String toJson() {
		return this.toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
	}

	public String toJson(JsonWriterSettings settings) {
		StringWriter writer = new StringWriter();
		(new MyBsonDocumentCodec()).encode(new JsonWriter(writer, settings), this, EncoderContext.builder().build());
		return writer.toString();
	}

	public boolean equals(Object o) {
		return this.toBaseBsonDocument().equals(o);
	}

	public int hashCode() {
		return this.toBaseBsonDocument().hashCode();
	}

	public BsonDocument clone() {
		return new RawBsonDocument((byte[]) this.bytes.clone(), this.offset, this.length);
	}

	private BsonBinaryReader createReader() {
		return new BsonBinaryReader(new ByteBufferBsonInput(this.getByteBuffer()));
	}

	private BsonDocument toBaseBsonDocument() {
		BsonBinaryReader bsonReader = this.createReader();

		BsonDocument var2;
		try {
			var2 = (new BsonDocumentCodec()).decode(bsonReader, DecoderContext.builder().build());
		}
		finally {
			bsonReader.close();
		}

		return var2;
	}

	private Object writeReplace() {
		return new MyBsonDocument.SerializationProxy(this.bytes, this.offset, this.length);
	}

	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Proxy required");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		private final byte[] bytes;

		SerializationProxy(byte[] bytes, int offset, int length) {
			if (bytes.length == length) {
				this.bytes = bytes;
			}
			else {
				this.bytes = new byte[length];
				System.arraycopy(bytes, offset, this.bytes, 0, length);
			}

		}

		private Object readResolve() {
			return new MyBsonDocument(this.bytes);
		}
	}

}
