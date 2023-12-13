package org.bson;

import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

public class MyBsonDocumentCodec implements Codec<MyBsonDocument>  {
	public MyBsonDocumentCodec() {
	}

	public void encode(BsonWriter writer, MyBsonDocument value, EncoderContext encoderContext) {
		BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(value.getByteBuffer()));

		try {
			writer.pipe(reader);
		} finally {
			reader.close();
		}

	}

	public MyBsonDocument decode(BsonReader reader, DecoderContext decoderContext) {
		BasicOutputBuffer buffer = new BasicOutputBuffer(0);
		BsonBinaryWriter writer = new BsonBinaryWriter(buffer);

		MyBsonDocument var5;
		try {
			writer.pipe(reader);
			var5 = new MyBsonDocument(buffer.getInternalBuffer(), 0, buffer.getPosition());
		} finally {
			writer.close();
			buffer.close();
		}

		return var5;
	}

	public Class<MyBsonDocument> getEncoderClass() {
		return MyBsonDocument.class;
	}

}
