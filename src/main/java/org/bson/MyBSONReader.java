package org.bson;

import org.bson.io.BsonInput;
import org.bson.io.BsonInputMark;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

public class MyBSONReader extends AbstractBsonReader {
	private final MyByteBufferBsonInput bsonInput = new MyByteBufferBsonInput();

	public MyBSONReader() {
	}

	public void reset(RawBsonDocument doc) {
		bsonInput.reset(doc.getByteBuffer());
		setState( AbstractBsonReader.State.INITIAL );
		this.setContext(new Context(null, BsonContextType.TOP_LEVEL, 0, 0));
	}

	public void close() {
	}

	public BsonInput getBsonInput() {
		return this.bsonInput;
	}

	public BsonType readBsonType() {
		if (this.isClosed()) {
			throw new IllegalStateException("BSONBinaryWriter");
		} else if (this.getState() != State.INITIAL && this.getState() != State.DONE && this.getState() != State.SCOPE_DOCUMENT) {
			if (this.getState() != State.TYPE) {
				this.throwInvalidState("ReadBSONType", State.TYPE);
			}

			final byte bsonTypeByte = this.bsonInput.readByte();
			final BsonType bsonType = BsonType.findByValue(bsonTypeByte);
			if (bsonType == null) {
				String name = this.bsonInput.readCString();
				throw new BsonSerializationException(String.format("Detected unknown BSON type \"\\x%x\" for fieldname \"%s\". Are you using the latest driver version?", bsonTypeByte, name));
			} else {
				this.setCurrentBsonType(bsonType);
				if (this.getCurrentBsonType() == BsonType.END_OF_DOCUMENT) {
					switch (this.getContext().getContextType()) {
						case ARRAY:
							this.setState(State.END_OF_ARRAY);
							return BsonType.END_OF_DOCUMENT;
						case DOCUMENT:
						case SCOPE_DOCUMENT:
							this.setState(State.END_OF_DOCUMENT);
							return BsonType.END_OF_DOCUMENT;
						default:
							throw new BsonSerializationException(String.format("BSONType EndOfDocument is not valid when ContextType is %s.", this.getContext().getContextType()));
					}
				} else {
					switch (this.getContext().getContextType()) {
						case ARRAY:
							this.bsonInput.skipCString();
							this.setState(State.VALUE);
							break;
						case DOCUMENT:
						case SCOPE_DOCUMENT:
							this.setCurrentName(this.bsonInput.readCString());
							this.setState(State.NAME);
							break;
						default:
							throw new BSONException("Unexpected ContextType.");
					}

					return this.getCurrentBsonType();
				}
			}
		} else {
			this.setCurrentBsonType(BsonType.DOCUMENT);
			this.setState(State.VALUE);
			return this.getCurrentBsonType();
		}
	}

	protected BsonBinary doReadBinaryData() {
		int numBytes = this.readSize();
		byte type = this.bsonInput.readByte();
		if (type == BsonBinarySubType.OLD_BINARY.getValue()) {
			int repeatedNumBytes = this.bsonInput.readInt32();
			if (repeatedNumBytes != numBytes - 4) {
				throw new BsonSerializationException("Binary sub type OldBinary has inconsistent sizes");
			}

			numBytes -= 4;
		}

		byte[] bytes = new byte[numBytes];
		this.bsonInput.readBytes(bytes);
		return new BsonBinary(type, bytes);
	}

	protected byte doPeekBinarySubType() {
		Mark mark = new Mark();
		this.readSize();
		byte type = this.bsonInput.readByte();
		mark.reset();
		return type;
	}

	protected int doPeekBinarySize() {
		Mark mark = new Mark();
		int size = this.readSize();
		mark.reset();
		return size;
	}

	protected boolean doReadBoolean() {
		byte booleanByte = this.bsonInput.readByte();
        /*if (booleanByte != 0 && booleanByte != 1) {
            throw new BsonSerializationException(String.format("Expected a boolean value but found %d", booleanByte));
        } else {*/
		return booleanByte == 1;
		//}
	}

	protected long doReadDateTime() {
		return this.bsonInput.readInt64();
	}

	protected double doReadDouble() {
		return this.bsonInput.readDouble();
	}

	protected int doReadInt32() {
		return this.bsonInput.readInt32();
	}

	protected long doReadInt64() {
		return this.bsonInput.readInt64();
	}

	public Decimal128 doReadDecimal128() {
		final long low = this.bsonInput.readInt64();
		final long high = this.bsonInput.readInt64();
		return Decimal128.fromIEEE754BIDEncoding(high, low);
	}

	protected String doReadJavaScript() {
		return this.bsonInput.readString();
	}

	protected String doReadJavaScriptWithScope() {
		int startPosition = this.bsonInput.getPosition();
		int size = this.readSize();
		this.setContext(new Context(this.getContext(), BsonContextType.JAVASCRIPT_WITH_SCOPE, startPosition, size));
		return this.bsonInput.readString();
	}

	protected void doReadMaxKey() {
	}

	protected void doReadMinKey() {
	}

	protected void doReadNull() {
	}

	@Override
	protected void checkPreconditions(String methodName, BsonType type) {
		this.verifyBSONType(methodName, type);
	}

	public byte[] readObjectId2() {
		this.checkPreconditions("readObjectId", BsonType.OBJECT_ID);
		this.setState(this.getNextState());
		return this.doReadObjectId2();
	}

	protected byte[] doReadObjectId2() {
		final byte[] bytes = new byte[12];
		this.bsonInput.readBytes(bytes);
		return bytes;
	}

	protected ObjectId doReadObjectId() {
		return this.bsonInput.readObjectId();
	}

	protected BsonRegularExpression doReadRegularExpression() {
		return new BsonRegularExpression(this.bsonInput.readCString(), this.bsonInput.readCString());
	}

	protected BsonDbPointer doReadDBPointer() {
		return new BsonDbPointer(this.bsonInput.readString(), this.bsonInput.readObjectId());
	}

	protected String doReadString() {
		return this.bsonInput.readString();
	}

	protected String doReadSymbol() {
		return this.bsonInput.readString();
	}

	protected BsonTimestamp doReadTimestamp() {
		return new BsonTimestamp(this.bsonInput.readInt64());
	}

	protected void doReadUndefined() {
	}

	public void doReadStartArray() {
		int startPosition = this.bsonInput.getPosition();
		int size = this.readSize();
		this.setContext(new Context(this.getContext(), BsonContextType.ARRAY, startPosition, size));
	}

	protected void doReadStartDocument() {
		BsonContextType contextType = this.getState() == State.SCOPE_DOCUMENT ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
		int startPosition = this.bsonInput.getPosition();
		int size = this.readSize();
		this.setContext(new Context(this.getContext(), contextType, startPosition, size));
	}

	protected void doReadEndArray() {
		this.setContext(this.getContext().popContext(this.bsonInput.getPosition()));
	}

	protected void doReadEndDocument() {
		this.setContext(this.getContext().popContext(this.bsonInput.getPosition()));
		if (this.getContext().getContextType() == BsonContextType.JAVASCRIPT_WITH_SCOPE) {
			this.setContext(this.getContext().popContext(this.bsonInput.getPosition()));
		}

	}

	protected void doSkipName() {
	}

	protected void doSkipValue() {
		if (this.isClosed()) {
			throw new IllegalStateException("BSONBinaryWriter");
		} else {
			if (this.getState() != State.VALUE) {
				this.throwInvalidState("skipValue", new State[]{State.VALUE});
			}

			int skip;
			switch (this.getCurrentBsonType()) {
				case ARRAY:
					skip = this.readSize() - 4;
					break;
				case BINARY:
					skip = this.readSize() + 1;
					break;
				case BOOLEAN:
					skip = 1;
					break;
				case DATE_TIME:
					skip = 8;
					break;
				case DOCUMENT:
					skip = this.readSize() - 4;
					break;
				case DOUBLE:
					skip = 8;
					break;
				case INT32:
					skip = 4;
					break;
				case INT64:
					skip = 8;
					break;
				case DECIMAL128:
					skip = 16;
					break;
				case JAVASCRIPT:
					skip = this.readSize();
					break;
				case JAVASCRIPT_WITH_SCOPE:
					skip = this.readSize() - 4;
					break;
				case MAX_KEY:
					skip = 0;
					break;
				case MIN_KEY:
					skip = 0;
					break;
				case NULL:
					skip = 0;
					break;
				case OBJECT_ID:
					skip = 12;
					break;
				case REGULAR_EXPRESSION:
					this.bsonInput.skipCString();
					this.bsonInput.skipCString();
					skip = 0;
					break;
				case STRING:
					skip = this.readSize();
					break;
				case SYMBOL:
					skip = this.readSize();
					break;
				case TIMESTAMP:
					skip = 8;
					break;
				case UNDEFINED:
					skip = 0;
					break;
				case DB_POINTER:
					skip = this.readSize() + 12;
					break;
				default:
					throw new BSONException("Unexpected BSON type: " + this.getCurrentBsonType());
			}

			this.bsonInput.skip(skip);
			this.setState(State.TYPE);
		}
	}

	private int readSize() {
		int size = this.bsonInput.readInt32();
		if (size < 0) {
			String message = String.format("Size %s is not valid because it is negative.", size);
			throw new BsonSerializationException(message);
		} else {
			return size;
		}
	}

	protected Context getContext() {
		return (Context) super.getContext();
	}

	public BsonReaderMark getMark() {
		return new Mark();
	}

	protected class Context extends org.bson.AbstractBsonReader.Context {
		private final int startPosition;
		private final int size;

		Context(Context parentContext, BsonContextType contextType, int startPosition, int size) {
			super(parentContext, contextType);
			this.startPosition = startPosition;
			this.size = size;
		}

		Context popContext(int position) {
			int actualSize = position - this.startPosition;
			if (actualSize != this.size) {
				throw new BsonSerializationException(String.format("Expected size to be %d, not %d.", this.size, actualSize));
			} else {
				return this.getParentContext();
			}
		}

		protected Context getParentContext() {
			return (Context) super.getParentContext();
		}
	}

	protected class Mark extends org.bson.AbstractBsonReader.Mark {
		private final int startPosition;
		private final int size;
		private final BsonInputMark bsonInputMark;

		protected Mark() {
			this.startPosition = getContext().startPosition;
			this.size = getContext().size;
			this.bsonInputMark = bsonInput.getMark(2147483647);
		}

		public void reset() {
			super.reset();
			this.bsonInputMark.reset();
			setContext(new Context((Context) this.getParentContext(), this.getContextType(), this.startPosition, this.size));
		}
	}
}
