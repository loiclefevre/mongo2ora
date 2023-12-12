package org.bson;

import org.bson.io.BsonInput;
import org.bson.io.BsonInputMark;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class MyByteBufferBsonInput implements BsonInput {
	private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	private static final String REPLACEMENT_STRING = UTF8_CHARSET.newDecoder().replacement();
	private static final String[] ONE_BYTE_ASCII_STRINGS = new String[128];
	private ByteBuf buffer;

	public MyByteBufferBsonInput() {
	}

	public void reset(ByteBuf buffer) {
		this.buffer = buffer;
	}

	public MyByteBufferBsonInput(ByteBuf buffer) {
		if (buffer == null) {
			throw new IllegalArgumentException("buffer can not be null");
		} else {
			this.buffer = buffer;
			buffer.order(ByteOrder.LITTLE_ENDIAN);
		}
	}

	public int getPosition() {
		return this.buffer.position();
	}

	public byte readByte() {
		return this.buffer.get();
	}

	public void readBytes(final byte[] bytes) {
		this.buffer.get(bytes);
	}

	public void readBytes(byte[] bytes, int offset, int length) {
		this.buffer.get(bytes, offset, length);
	}

	public long readInt64() {
		return this.buffer.getLong();
	}

	public double readDouble() {
		return this.buffer.getDouble();
	}

	public int readInt32() {
		return this.buffer.getInt();
	}

	private final byte[] bytes = new byte[12];
	public ObjectId readObjectId() {
		this.readBytes(bytes,0,12);
		return new ObjectId(ByteBuffer.wrap(bytes));
	}

	public String readString() {
		final int size = this.readInt32();
        /*if (size <= 0) {
            throw new BsonSerializationException(String.format("While decoding a BSON string found a size that is not a positive number: %d", size));
        } else {*/
		return this.readString(size);
		//}
	}

	public String readCString() {
		int mark = this.buffer.position();
		this.skipCString();
		int size = this.buffer.position() - mark;
		this.buffer.position(mark);
		return this.readString(size);
	}

	private final byte[] readStringBuffer = new byte[1024*1024];

	private final String readString(final int size) {
		//byte nullByte;
		if (size == 2) {
			final byte asciiByte = this.buffer.get();
			/*nullByte =*/ this.buffer.get();
            /*if (nullByte != 0) {
                throw new BsonSerializationException("Found a BSON string that is not null-terminated");
            } else {*/
			return asciiByte < 0 ? REPLACEMENT_STRING : ONE_BYTE_ASCII_STRINGS[asciiByte];
			//}
		} else {
//			final byte[] bytes = new byte[size - 1];
//			this.buffer.get(bytes);
//			/*nullByte =*/ this.buffer.get();
//            /*if (nullByte != 0) {
//                throw new BsonSerializationException("Found a BSON string that is not null-terminated");
//            } else {*/
//			return new String(bytes, 0, size-1, UTF8_CHARSET);
//			//}

			this.buffer.get(readStringBuffer,0,size);
			//this.buffer.get(); // read null
			return new String(readStringBuffer, 0, size-1, UTF8_CHARSET);
		}
	}

	public final void skipCString() {
/*
		for(boolean checkNext = true; checkNext; checkNext = this.buffer.get() != 0) {
			if (!this.buffer.hasRemaining()) {
				throw new BsonSerializationException("Found a BSON string that is not null-terminated");
			}
		}
*/
		final ByteBuf b = this.buffer;
		final int rem = b.remaining();
		for(int i = 0; i < rem; i++) {
			if( b.get() == 0 ) return;
		}

		throw new BsonSerializationException("Found a BSON string that is not null-terminated");
	}

	public void skip(int numBytes) {
		this.buffer.position(this.buffer.position() + numBytes);
	}

	public BsonInputMark getMark(int readLimit) {
		return new BsonInputMark() {
			private int mark;

			{
				this.mark = MyByteBufferBsonInput.this.buffer.position();
			}

			public void reset() {
				MyByteBufferBsonInput.this.buffer.position(this.mark);
			}
		};
	}

	public boolean hasRemaining() {
		return this.buffer.hasRemaining();
	}

	public void close() {
		this.buffer.release();
		this.buffer = null;
	}

	static {
		for(int b = 0; b < ONE_BYTE_ASCII_STRINGS.length; ++b) {
			ONE_BYTE_ASCII_STRINGS[b] = String.valueOf((char)b);
		}

	}
}

