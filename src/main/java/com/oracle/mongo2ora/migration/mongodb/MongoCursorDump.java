package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.bson.RawBsonDocument;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class MongoCursorDump<TResult> implements MongoCursor<TResult> {
	public final FindIterableDump<TResult> findIterable;
	public final long count;
	public long current;
	public InputStream inputStream;

	public MongoCursorDump(FindIterableDump<TResult> findIterable) {
		this.findIterable=findIterable;
		this.count=findIterable.mongoCollectionDump.work.count;
		File collectionData= new File(findIterable.mongoCollectionDump.sourceDumpFolder,findIterable.mongoCollectionDump.name+".bson");
		if(!collectionData.exists()) {
			collectionData= new File(findIterable.mongoCollectionDump.sourceDumpFolder,findIterable.mongoCollectionDump.name+".bson.gz");
			if(!collectionData.exists()) return;
		}

		try
		{
			inputStream = collectionData.getName().toLowerCase().endsWith(".gz") ?
					new GZIPInputStream(new FileInputStream(collectionData), 128 * 1024 * 1024)
					: new BufferedInputStream(new FileInputStream(collectionData), 128 * 1024 * 1024);
			inputStream.skipNBytes(findIterable.mongoCollectionDump.work.startPosition);

/*			while (true) {
				try {
					//final byte[] data = readNextBSONRawData(inputStream);
					skipNextBSONRawData(inputStream);
					current++;

				} catch (EOFException eof) {
					break;
				}
			}*/
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private final static byte[] bsonDataSize = new byte[4];

	private static byte[] readNextBSONRawData(InputStream input) throws IOException {
		int readBytes = input.read(bsonDataSize, 0, 4);
		if (readBytes != 4) throw new EOFException();

		final int bsonSize = (bsonDataSize[0] & 0xff) |
				((bsonDataSize[1] & 0xff) << 8) |
				((bsonDataSize[2] & 0xff) << 16) |
				((bsonDataSize[3] & 0xff) << 24);

		final byte[] rawData = new byte[bsonSize];

		System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

		for (int i = bsonSize - 4, off = 4; i > 0; off += readBytes) {
			readBytes = input.read(rawData, off, i);
			if (readBytes < 0) {
				throw new EOFException();
			}

			i -= readBytes;
		}

		return rawData;
	}

	@Override
	public void close() {
		try {
			if(inputStream != null) {
				inputStream.close();
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return current < count;
	}

	@Override
	public TResult next() {
		try {
			current++;
			final byte[] data = readNextBSONRawData(inputStream);

			return (TResult)new RawBsonDocument(data);
		}
		catch (IOException eof) {
		}

		return null;
	}

	@Override
	public int available() {
		return 0;
	}

	@Override
	public TResult tryNext() {
		return null;
	}

	@Override
	public ServerCursor getServerCursor() {
		return null;
	}

	@Override
	public ServerAddress getServerAddress() {
		return null;
	}
}
