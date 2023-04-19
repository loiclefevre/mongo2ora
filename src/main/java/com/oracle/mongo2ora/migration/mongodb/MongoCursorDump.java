package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.operation.BatchCursor;
import org.bson.RawBsonDocument;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MongoCursorDump<TResult> implements BatchCursor<TResult> {
	private static final Logger LOGGER = Loggers.getLogger("MongoCursorDump");
	public final FindIterableDump<TResult> findIterable;
	public final long count;
	public long current;
	public InputStream inputStream;

	public MongoCursorDump(FindIterableDump<TResult> findIterable) {
		this.findIterable = findIterable;
		this.count = findIterable.mongoCollectionDump.work.count;
		File collectionData = new File(findIterable.mongoCollectionDump.sourceDumpFolder, findIterable.mongoCollectionDump.name + ".bson");
		if (!collectionData.exists()) {
			collectionData = new File(findIterable.mongoCollectionDump.sourceDumpFolder, findIterable.mongoCollectionDump.name + ".bson.gz");
			if (!collectionData.exists()) return;
		}

		/*try {
			RandomAccessFile file = new RandomAccessFile(collectionData,"r");
			FileChannel fileChannel = file.getChannel();
			MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}*/

		try {
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
			LOGGER.info("Collection " + findIterable.mongoCollectionDump.name + " has " + count + " documents.");
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
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

	@Override
	public void close() {
		try {
			if (inputStream != null) {
				LOGGER.info("Collection " + findIterable.mongoCollectionDump.name + " closing inputStream.");
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

	private final List<TResult> EMPTY_LIST = new ArrayList<>();
	@Override
	public List<TResult> next() {
		try {
			final List<TResult> results = new ArrayList<>(getBatchSize());

			int i = 0;
			while(hasNext() && i < getBatchSize()) {
				current++;
				final byte[] data = readNextBSONRawData(inputStream);
				results.add((TResult)new RawBsonDocument(data, 0, data.length));
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
