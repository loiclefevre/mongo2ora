package com.oracle.mongo2ora.migration.converter;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.ConversionInformation;
import com.oracle.mongo2ora.migration.mongodb.CollectionCluster;
import oracle.rsi.PushPublisher;
import oracle.rsi.RSIException;
import oracle.rsi.ReactiveStreamsIngestion;
import oracle.sql.RAW;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;

import java.sql.Blob;
import java.sql.Timestamp;
import java.util.concurrent.CompletableFuture;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class RSIBSON2OSONCollectionConverter implements Runnable {
	private static final Logger LOGGER = Loggers.getLogger("converter");

	private final CollectionCluster work;
	private final CompletableFuture<ConversionInformation> publishingCf;

	private final ReactiveStreamsIngestion rsi;
	private final MongoDatabase database;
	private final int partitionId;
	private final ASCIIGUI gui;
	private final int batchSize;
	private final String collectionName;

	public RSIBSON2OSONCollectionConverter(int partitionId, String collectionName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, ReactiveStreamsIngestion rsi, ASCIIGUI gui, int batchSize) {
		this.partitionId = partitionId;
		this.collectionName = collectionName;
		this.work = work;
		this.publishingCf = publishingCf;
		this.database = database;
		this.rsi = rsi;
		this.gui = gui;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
		long bsonLength = 0;
		long osonLength = 0;
		long count = 0;
		final int threadId = Integer.parseInt(Thread.currentThread().getName());

		//System.out.println("Thread " + threadId + " working on " + work.count + " docs");

		try {
			MongoCollection<RawBsonDocument> collection = database.getCollection(collectionName, RawBsonDocument.class);

			try (MongoCursor<RawBsonDocument> cursor = collection.find(Filters.and(
							Filters.gte("_id", work.minId),
							Filters.lt("_id", work.maxId)
					)
			).hint(useIdIndexHint).batchSize(2048).cursor()) {
				long start = System.currentTimeMillis();

				// Reactive Streaming Ingestion


				final MyBSONDecoder decoder = new MyBSONDecoder(true);

				//final PushPublisher<Object[]> pushPublisher = ReactiveStreamsIngestion.pushPublisher();
				final MyPushPublisher<Object[]> pushPublisher = new MyPushPublisher<>();
				pushPublisher.subscribe(rsi.subscriber());

				long memPressureCount = 0;

				long mongoDBFetchStart;
				long mongoDBFetch = 0;
				long bsonConvertStart;
				long bsonConvert = 0;
				long serializeOSONStart;
				long serializeOSON = 0;
				long publishStart;
				long publish = 0;


				while (cursor.hasNext()) {
					//out.reset();
					mongoDBFetchStart = System.nanoTime();
					final RawBsonDocument doc = cursor.next();
					mongoDBFetch += (System.nanoTime() - mongoDBFetchStart);

					// -500 MB/sec
					bsonConvertStart = System.nanoTime();
					decoder.convertBSONToOSON(doc);
					bsonConvert += (System.nanoTime() - bsonConvertStart);
					bsonLength += decoder.getBsonLength();

					serializeOSONStart = System.nanoTime();
					final byte[] osonData = decoder.getOSONData();
					serializeOSON += (System.nanoTime() - serializeOSONStart);

					publishStart = System.nanoTime();
					final Timestamp time = new java.sql.Timestamp(System.currentTimeMillis());
					pushPublisher.accept(new Object[]{decoder.getOid(), time, time, "1", new MyBLOB(osonData)});
					publish += (System.nanoTime() - publishStart);

					count++;
					osonLength += osonData.length;
				}

				pushPublisher.close();

				LOGGER.info("count=" + count + ", mongoDBFetch=" + mongoDBFetch + ", bsonConvert=" + bsonConvert + ", serializeOSON=" + serializeOSON + ", publish=" + publish);

				//final long duration = System.currentTimeMillis() - start;
				gui.updateDestinationDatabaseDocuments(count, osonLength);
				//System.out.println("Thread " + threadId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
			}
		}
		catch (Exception e) {
			LOGGER.error("RSI Publishing", e);
			publishingCf.complete(new ConversionInformation(e));
		}
		finally {
			//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
			publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
		}
	}
}
