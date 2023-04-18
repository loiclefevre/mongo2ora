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
import oracle.jdbc.driver.DPRowBinder2;
import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;

import java.sql.Connection;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class DirectDirectPathBSON2OSONCollectionConverter implements Runnable {
	private static final Logger LOGGER = Loggers.getLogger("converter");
	private final Semaphore DB_SEMAPHORE;

	private final CollectionCluster work;
	private final CompletableFuture<ConversionInformation> publishingCf;
	private final PoolDataSource pds;
	private final MongoDatabase database;
	private final int partitionId;
	private final ASCIIGUI gui;
	private final int batchSize;
	private final String collectionName;

	public DirectDirectPathBSON2OSONCollectionConverter(int partitionId, String collectionName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, PoolDataSource pds, ASCIIGUI gui, int batchSize, Semaphore DB_SEMAPHORE) {
		this.partitionId = partitionId;
		this.collectionName = collectionName;
		this.work = work;
		this.publishingCf = publishingCf;
		this.database = database;
		this.pds = pds;
		this.gui = gui;
		this.batchSize = batchSize;
		this.DB_SEMAPHORE = DB_SEMAPHORE;
	}

	@Override
	public void run() {
		long bsonLength = 0;
		long osonLength = 0;
		long count = 0;
		//final int threadId = Integer.parseInt(Thread.currentThread().getName());

		//System.out.println("Thread " + threadId + " working on " + work.count + " docs");


		try {
			DB_SEMAPHORE.acquire();

			MongoCollection<RawBsonDocument> collection = database.getCollection(collectionName, RawBsonDocument.class);
			try (Connection c = pds.getConnection()) {
				c.setAutoCommit(false);

                    /*
                    try (Statement s = c.createStatement()) {
                        try (ResultSet r = s.executeQuery("select sys_context('USERENV','INSTANCE') from dual")) {
                            if (r.next()) {
                                System.out.println("Starting job with thread id: " + Thread.currentThread().getName() + ", partition id: " + partitionId + " on instance id: " + r.getInt(1));
                            }
                        }
                    }
                    */

				final OracleConnection realConnection = (OracleConnection) c;

				try (MongoCursor<RawBsonDocument> cursor = collection.find(Filters.and(
								Filters.gte("_id", work.minId),
								Filters.lt("_id", work.maxId)
						)
				).hint(useIdIndexHint).batchSize(2048 /*batchSize*2*/).cursor()) {
					//long start = System.currentTimeMillis();

					final EnumSet<OracleConnection.CommitOption> commitOptions = EnumSet.of(
							OracleConnection.CommitOption.WRITEBATCH,
							OracleConnection.CommitOption.NOWAIT);

					final byte[] version = "1".getBytes();

					try (DPRowBinder2 p = new DPRowBinder2( c, pds.getUser().toUpperCase(), collectionName, null, new String[]{"ID", "VERSION", "JSON_DOCUMENT"} /* String.format("p%d", partitionId),*/)) {
						final MyBSONDecoder decoder = new MyBSONDecoder(true);

						while (cursor.hasNext()) {
							final RawBsonDocument doc = cursor.next();
							decoder.convertBSONToOSON(doc);
							bsonLength += decoder.getBsonLength();

							final byte[] osonData= decoder.getOSONData();
							osonLength += osonData.length;

							p.beginNew();
							p.append(decoder.getOid());
							p.append(version);
							p.append(osonData);
							p.finish();
							count++;
						}

						p.flushData();

						realConnection.commit(commitOptions);

						//LOGGER.info("count=" + count + ", mongoDBFetch=" + mongoDBFetch + ", bsonConvert=" + bsonConvert + ", serializeOSON=" + serializeOSON + ", addBatch=" + addBatch + ", jdbcBatchExecute=" + jdbcBatchExecute);

						//final long duration = System.currentTimeMillis() - start;
						gui.updateDestinationDatabaseDocuments(count, osonLength);
						//System.out.println("Thread " + threadId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
					}
				}
			}
		}
		catch (Exception e) {
			//e.printStackTrace();
			publishingCf.complete(new ConversionInformation(e));
		}
		finally {
			//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
			DB_SEMAPHORE.release();
			publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
		}
	}
}
