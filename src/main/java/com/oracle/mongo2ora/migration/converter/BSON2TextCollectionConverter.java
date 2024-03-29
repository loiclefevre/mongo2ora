package com.oracle.mongo2ora.migration.converter;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.ConversionInformation;
import com.oracle.mongo2ora.migration.mongodb.CollectionCluster;
import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class BSON2TextCollectionConverter implements Runnable {
	private final CollectionCluster work;
	private final CompletableFuture<ConversionInformation> publishingCf;
	private final PoolDataSource pds;
	private final MongoDatabase database;
	private final int partitionId;
	private final ASCIIGUI gui;
	private final int batchSize;
	private final String collectionName;
	private final String tableName;

	public BSON2TextCollectionConverter(int partitionId, String collectionName, String tableName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, PoolDataSource pds, ASCIIGUI gui, int batchSize) {
		this.partitionId = partitionId;
		this.collectionName = collectionName;
		this.tableName = tableName;
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
		final int threadId = Integer.parseInt(Thread.currentThread().getName());

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

					//try (PreparedStatement p = ((OracleConnection) c).prepareDirectPath(pds.getUser().toUpperCase(), collectionName, new String[]{"ID", "VERSION", "JSON_DOCUMENT"},/* String.format("p%d", partitionId),*/ directPathLoadProperties)) {
					try (PreparedStatement p = c.prepareStatement("insert /*+ append */ into \"" + tableName + "\" (ID, VERSION, JSON_DOCUMENT) values (?,?,?)")) {

						p.setString(2, "1");

						int batchSizeCounter = 0;

						final MyBSONDecoder decoder = new MyBSONDecoder(true);

						while (cursor.hasNext()) {
							//out.reset();
							final RawBsonDocument doc = cursor.next();

							// -500 MB/sec
							decoder.convertBSONToOSON(doc);
							bsonLength += decoder.getBsonLength();

							byte[] osonData;
//								p.setBytes(3, osonData = decoder.getOSONData());
//								p.setObject(3,osonData = decoder.getOSONData(), OracleTypes.JSON);
							final String json = doc.toJson();
							p.setString(3, json);
							osonData = json.getBytes(StandardCharsets.UTF_8);

							p.setString(1, decoder.getOid());
							//final CHAR oid = new CHAR(decoder.getOid(), cs);

							//p.setObject(1, oid, oracle.jdbc.OracleTypes.CHAR);


							osonLength += osonData.length;

							p.addBatch();

							batchSizeCounter++;

							if (batchSizeCounter >= batchSize) {
								count += batchSizeCounter;
								p.executeLargeBatch();
								batchSizeCounter = 0;
							}
						}

						if (batchSizeCounter > 0) {
							count += batchSizeCounter;
							p.executeLargeBatch();
						}

						realConnection.commit(commitOptions);

						//final long duration = System.currentTimeMillis() - start;
						gui.updateDestinationDatabaseDocuments(count, osonLength);
						//System.out.println("Thread " + threadId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
					}
				}
			}
		}
		catch (SQLException sqle) {
			publishingCf.complete(new ConversionInformation(sqle));
		}
		finally {
			//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
			publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
		}
	}
}