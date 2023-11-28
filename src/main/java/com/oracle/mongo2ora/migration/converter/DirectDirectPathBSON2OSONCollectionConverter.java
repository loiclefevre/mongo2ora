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
import com.oracle.mongo2ora.migration.mongodb.MongoCollectionDump;
import oracle.jdbc.driver.DPRowBinder2;
import oracle.jdbc.internal.OracleConnection;
import oracle.json.util.HashFuncs;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;

import java.sql.Connection;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class DirectDirectPathBSON2OSONCollectionConverter implements Runnable {
	private static final Logger LOGGER = Loggers.getLogger("converter");
	private final Semaphore DB_SEMAPHORE;

	private final Semaphore GUNZIP_SEMAPHORE;

	private final CollectionCluster work;
	private final CompletableFuture<ConversionInformation> publishingCf;
	private final PoolDataSource pds;
	private final MongoDatabase database;
	private final int partitionId;
	private final ASCIIGUI gui;
	private final int batchSize;
	private final String collectionName;

	private final boolean mongoDBAPICompatible;
	private final String tableName;
	private final int oracleDBVersion;
	private final Properties collectionsProperties;
	private final boolean allowDuplicateKeys;

	public DirectDirectPathBSON2OSONCollectionConverter(int partitionId, String collectionName, String tableName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, PoolDataSource pds, ASCIIGUI gui, int batchSize, Semaphore DB_SEMAPHORE, boolean mongoDBAPICompatible, int oracleDBVersion, Properties collectionsProperties, boolean allowDuplicateKeys, Semaphore GUNZIP_SEMAPHORE) {
		this.partitionId = partitionId;
		this.collectionName = collectionName;
		this.tableName = tableName;
		this.work = work;
		this.publishingCf = publishingCf;
		this.database = database;
		this.pds = pds;
		this.gui = gui;
		this.batchSize = batchSize;
		this.DB_SEMAPHORE = DB_SEMAPHORE;
		this.mongoDBAPICompatible = mongoDBAPICompatible;
		this.oracleDBVersion = oracleDBVersion;
		this.collectionsProperties = collectionsProperties;
		this.allowDuplicateKeys = allowDuplicateKeys;
		this.GUNZIP_SEMAPHORE = GUNZIP_SEMAPHORE;
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

				if (work.sourceDump) {
					((MongoCollectionDump<RawBsonDocument>) collection).setWork(work);
				}

				final OracleConnection realConnection = (OracleConnection) c;

				try (MongoCursor<RawBsonDocument> cursor = collection.find(Filters.and(
								Filters.gte("_id", work.minId),
								Filters.lt("_id", work.maxId)
						)
				).hint(useIdIndexHint).batchSize(batchSize).cursor()) {
					long start = System.currentTimeMillis();

					final EnumSet<OracleConnection.CommitOption> commitOptions = EnumSet.of(
							OracleConnection.CommitOption.WRITEBATCH,
							OracleConnection.CommitOption.NOWAIT);

					final byte[] version = "1".getBytes();

					LOGGER.warn("Preparing direct path API with table: "+tableName+ " (MongoDB API compatible: "+this.mongoDBAPICompatible+")");

					if (this.mongoDBAPICompatible || oracleDBVersion >= 23) {
						LOGGER.warn("Loading table "+tableName+" with MongoDB API compatibility with VERSION and DATA...");

						String IDproperty = collectionsProperties.getProperty(collectionName+".ID","EMBEDDED_OID");

						LOGGER.info("Columns used for Direct Path API: "+("EMBEDDED_OID".equalsIgnoreCase(IDproperty) ? "VERSION, DATA" : "ID, VERSION, DATA"));

						try (DPRowBinder2 p = new DPRowBinder2(c, pds.getUser().toUpperCase(), "\"" + tableName + "\"", null,
								"EMBEDDED_OID".equalsIgnoreCase(IDproperty) ?
								new String[]{"VERSION", "DATA"} : new String[]{"ID", "VERSION", "DATA"} /* String.format("p%d", partitionId),*/)) {
							final MyBSONDecoder decoder = new MyBSONDecoder(true, allowDuplicateKeys);

							if("EMBEDDED_OID".equalsIgnoreCase(IDproperty)) {
								// ID column is filled using path expression from document content (usually $._id)
								while (cursor.hasNext()) {
									final RawBsonDocument doc = cursor.next();
									decoder.convertBSONToOSON(doc);
									bsonLength += decoder.getBsonLength();

									final byte[] osonData = decoder.getOSONData();
									osonLength += osonData.length;

									p.beginNew();
									p.append(version);
									p.append(osonData);
									p.finish();
									count++;
								}
							} else {
								// ID column is filled using generated value either present from the document or in the case there is none from a random generator
								final HashFuncs uuidGenerator = new HashFuncs();

								while (cursor.hasNext()) {
									final RawBsonDocument doc = cursor.next();
									decoder.convertBSONToOSON(doc);
									bsonLength += decoder.getBsonLength();

									final byte[] osonData = decoder.getOSONData();
									osonLength += osonData.length;

									p.beginNew();
									if(decoder.hasOid()) {
										p.append(decoder.getOid());
									} else {
										p.append(uuidGenerator.getRandom());
									}
									p.append(version);
									p.append(osonData);
									p.finish();
									count++;
								}
							}

							p.flushData();

							realConnection.commit(commitOptions);
						}
					}
					else {
						try (DPRowBinder2 p = new DPRowBinder2(c, pds.getUser().toUpperCase(), "\"" + tableName + "\"", null, new String[]{"ID", "VERSION", "JSON_DOCUMENT"} /* String.format("p%d", partitionId),*/)) {
							final MyBSONDecoder decoder = new MyBSONDecoder(true, allowDuplicateKeys);

							while (cursor.hasNext()) {
								final RawBsonDocument doc = cursor.next();
								decoder.convertBSONToOSON(doc);
								bsonLength += decoder.getBsonLength();

								final byte[] osonData = decoder.getOSONData();
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
						}
					}


					//LOGGER.info("count=" + count + ", mongoDBFetch=" + mongoDBFetch + ", bsonConvert=" + bsonConvert + ", serializeOSON=" + serializeOSON + ", addBatch=" + addBatch + ", jdbcBatchExecute=" + jdbcBatchExecute);

					final long duration = System.currentTimeMillis() - start;
					gui.updateDestinationDatabaseDocuments(count, osonLength);
					LOGGER.info("Thread " + partitionId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
				}
			}
		}
		catch (
				Exception e) {
			//e.printStackTrace();
			publishingCf.complete(new ConversionInformation(e));
		}
		finally {
			//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
			DB_SEMAPHORE.release();
//			try {
//				Thread.sleep(5000L);
//			}
//			catch (InterruptedException e) {
//				throw new RuntimeException(e);
//			}
			if(GUNZIP_SEMAPHORE != null) GUNZIP_SEMAPHORE.release();
			publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
		}
	}
}
