package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.operation.BatchCursor;
import org.bson.RawBsonDocument;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class MongoCursorDump<TResult> implements BatchCursor<TResult> {
	private static final Logger LOGGER = Loggers.getLogger("MongoCursorDump");
	public final FindIterableDump<TResult> findIterable;
	public final long count;
	public long current;
	//public InputStream inputStream;
	public RandomAccessFile file;
	public FileChannel fileChannel;
	public ByteBuffer buffer;

	public boolean gzipped;

	public MongoCursorDump(FindIterableDump<TResult> findIterable) {
		this.findIterable = findIterable;
		this.count = findIterable.mongoCollectionDump.work.count;
		File collectionData = new File(findIterable.mongoCollectionDump.sourceDumpFolder, findIterable.mongoCollectionDump.name + ".bson");
		if (!collectionData.exists()) {
			collectionData = new File(findIterable.mongoCollectionDump.sourceDumpFolder, findIterable.mongoCollectionDump.name + ".bson.gz");
			if (!collectionData.exists()) return;
			gzipped = true;
		}

		if(gzipped) {
			buffer = findIterable.mongoCollectionDump.work.buffer;
		} else {
			// https://howtodoinjava.com/java/nio/memory-mapped-files-mappedbytebuffer/#:~:text=2.-,Java%20Memory%2DMapped%20Files,as%20a%20very%20large%20array.
			try {
				LOGGER.info("Collection " + findIterable.mongoCollectionDump.name + " has " + count + " documents (skept " + findIterable.mongoCollectionDump.work.startPosition + " bytes, map " + findIterable.mongoCollectionDump.work.rawSize + " bytes in RAM).");
				file = new RandomAccessFile(collectionData, "r");
				fileChannel = file.getChannel();
				buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, findIterable.mongoCollectionDump.work.startPosition, findIterable.mongoCollectionDump.work.rawSize);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

/*		try {
			inputStream = collectionData.getName().toLowerCase().endsWith(".gz") ?
					new GZIPInputStream(new FileInputStream(collectionData), 128 * 1024 * 1024)
					: new BufferedInputStream(new FileInputStream(collectionData), 128 * 1024 * 1024);

			inputStream.skipNBytes(findIterable.mongoCollectionDump.work.startPosition);

			LOGGER.info("Collection " + findIterable.mongoCollectionDump.name + " has " + count + " documents (skept "+findIterable.mongoCollectionDump.work.startPosition+" bytes).");
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}*/
	}

	private final byte[] bsonDataSize = new byte[4];

	private byte[] readNextBSONRawData(InputStream input) throws IOException {
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

	private byte[] readNextBSONRawData() throws IOException {
		if (buffer.remaining() < 4) throw new EOFException();
		buffer.get(bsonDataSize, 0, 4);

		final int bsonSize = (bsonDataSize[0] & 0xff) |
				((bsonDataSize[1] & 0xff) << 8) |
				((bsonDataSize[2] & 0xff) << 16) |
				((bsonDataSize[3] & 0xff) << 24);

		final byte[] rawData = new byte[bsonSize];

		System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

		final int remainingBytesToRead = bsonSize - 4;
		if (buffer.remaining() < remainingBytesToRead)
			throw new EOFException();
		buffer.get(rawData, 4, remainingBytesToRead);

		return rawData;
	}

	@Override
	public void close() {
		try {
			/*
			if (inputStream != null) {
				//LOGGER.info("Collection " + findIterable.mongoCollectionDump.name + " closing inputStream.");
				inputStream.close();
			}*/
			if(buffer != null) {
				buffer = null;
			}
			if (fileChannel != null) {
				fileChannel.close();
			}
			if (file != null) {
				file.close();
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

	private final List<TResult> EMPTY_LIST = new ArrayList<>();

	@Override
	public List<TResult> next() {
		try {
			final List<TResult> results = new ArrayList<>(getBatchSize());

			int i = 0;
			while (hasNext() && i < getBatchSize()) {
				current++;
				final byte[] data = readNextBSONRawData();
				results.add((TResult) new RawBsonDocument(data, 0, data.length));
				i++;
			}


			/*if (current % 10000 == 0) {
				LOGGER.info("cursor created " + current + " documents");
			}*/

			return results;
		}
		catch (IOException eof) {
		}

		return EMPTY_LIST;
	}

	@Override
	public int available() {
		LOGGER.warn(">>>>>>>>>>>>    available");
		return 0;
	}

	@Override
	public void setBatchSize(int i) {

	}

	@Override
	public int getBatchSize() {
		return 2048;
	}

	@Override
	public List<TResult> tryNext() {
		LOGGER.warn(">>>>>>>>>>>>    tryNext");
		return null;
	}

	@Override
	public ServerCursor getServerCursor() {
		LOGGER.warn(">>>>>>>>>>>>    getServerCursor");
		return null;
	}

	@Override
	public ServerAddress getServerAddress() {
		LOGGER.warn(">>>>>>>>>>>>    getServerAddress");
		return null;
	}
}
