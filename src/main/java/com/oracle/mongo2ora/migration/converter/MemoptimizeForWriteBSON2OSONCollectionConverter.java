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
import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class MemoptimizeForWriteBSON2OSONCollectionConverter implements Runnable {
	private static final Logger LOGGER = Loggers.getLogger("converter");

	private final CollectionCluster work;
	private final CompletableFuture<ConversionInformation> publishingCf;
	private final PoolDataSource pds;
	private final MongoDatabase database;
	private final int partitionId;
	private final ASCIIGUI gui;
	private final int batchSize;
	private final String collectionName;

	public MemoptimizeForWriteBSON2OSONCollectionConverter(int partitionId, String collectionName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, PoolDataSource pds, ASCIIGUI gui, int batchSize) {
		this.partitionId = partitionId;
		this.collectionName = collectionName;
		this.work = work;
		this.publishingCf = publishingCf;
		this.database = database;
		this.pds = pds;
		this.gui = gui;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
		long bsonLength = 0;
		long osonLength = 0;
		long count = 0;
		//final int threadId = Integer.parseInt(Thread.currentThread().getName());

		//System.out.println("Thread " + threadId + " working on " + work.count + " docs");

		try {
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
							oracle.jdbc.internal.OracleConnection.CommitOption.WRITEBATCH,
							oracle.jdbc.internal.OracleConnection.CommitOption.NOWAIT);

					// Reactive Streaming Ingestion

					try (PreparedStatement p = c.prepareStatement("insert /*+ memoptimize_write */ into " + collectionName + " (ID, VERSION, JSON_DOCUMENT) values (?,?,?)")) {
                            /*final CharacterSet cs = CharacterSet.make(CharacterSet.AL32UTF8_CHARSET);
                            final CHAR version = new CHAR("1", cs);

                            p.setObject(3, version, oracle.jdbc.OracleTypes.CHAR); */
						p.setString(2, "1");

						int batchSizeCounter = 0;

						//final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
						//final MyStringReader msr = new MyStringReader("");

						final MyBSONDecoder decoder = new MyBSONDecoder(true);

//						long mongoDBFetchStart;
//						long mongoDBFetch = 0;
//						long bsonConvertStart;
//						long bsonConvert = 0;
//						long serializeOSONStart;
//						long serializeOSON = 0;
//						long jdbcBatchExecuteStart;
//						long jdbcBatchExecute = 0;
//						long addBatchStart;
//						long addBatch = 0;

						while (cursor.hasNext()) {
							//out.reset();
//							mongoDBFetchStart = System.nanoTime();
							final RawBsonDocument doc = cursor.next();
//							mongoDBFetch += (System.nanoTime() - mongoDBFetchStart);

							// -500 MB/sec
//							bsonConvertStart = System.nanoTime();
							decoder.convertBSONToOSON(doc);
//							bsonConvert += (System.nanoTime() - bsonConvertStart);
							bsonLength += decoder.getBsonLength();

//                                OracleJsonGenerator ogen = factory.createJsonBinaryGenerator(out);
							//JsonGenerator gen = ogen.wrap(JsonGenerator.class);
//                                msr.reset("{\"test\":1}");
							//msr.reset(doc.toJson(jsonWriterSettings));
//                                ogen.writeParser(factory.createJsonTextParser(msr));
//                                ogen.close();

//                                p.setString(1, doc.get("_id").toString());
//                                p.setBytes(2, out.toByteArray());
							byte[] osonData;
//							serializeOSONStart = System.nanoTime();
							p.setBytes(3, osonData = decoder.getOSONData());
//							serializeOSON += (System.nanoTime() - serializeOSONStart);
//								p.setObject(3,osonData = decoder.getOSONData(), OracleTypes.JSON);
//								p.setString(3, doc.toJson());
//								osonData = doc.toJson().getBytes(StandardCharsets.UTF_8);


							p.setString(1, decoder.getOid());
							//final CHAR oid = new CHAR(decoder.getOid(), cs);

							//p.setObject(1, oid, oracle.jdbc.OracleTypes.CHAR);


							osonLength += osonData.length;

//							addBatchStart = System.nanoTime();
							p.addBatch();
//							addBatch += (System.nanoTime() - addBatchStart);

							batchSizeCounter++;

							if (batchSizeCounter >= batchSize) {
								count += batchSizeCounter;
//								jdbcBatchExecuteStart = System.nanoTime();
								p.executeLargeBatch();
//								jdbcBatchExecute += (System.nanoTime() - jdbcBatchExecuteStart);
								batchSizeCounter = 0;
							}
						}

						if (batchSizeCounter > 0) {
							count += batchSizeCounter;
							//jdbcBatchExecuteStart = System.nanoTime();
							p.executeLargeBatch();
							//jdbcBatchExecute += (System.nanoTime() - jdbcBatchExecuteStart);
						}

						realConnection.commit(commitOptions);

						//LOGGER.info("count=" + count + ", mongoDBFetch=" + mongoDBFetch + ", bsonConvert=" + bsonConvert + ", serializeOSON=" + serializeOSON + ", addBatch=" + addBatch + ", jdbcBatchExecute=" + jdbcBatchExecute);

						//final long duration = System.currentTimeMillis() - start;
						gui.updateDestinationDatabaseDocuments(count, osonLength);
						//System.out.println("Thread " + threadId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
					}
				}
			}
		}
		catch (SQLException sqle) {
			//sqle.printStackTrace();
			publishingCf.complete(new ConversionInformation(sqle));
		}
		finally {
			//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
			publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
		}
	}
}
