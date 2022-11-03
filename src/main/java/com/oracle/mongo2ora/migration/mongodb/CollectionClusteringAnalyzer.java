package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.Filters;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.concurrent.CompletableFuture;

// TODO: dychotomic search
// TODO: see https://github.com/mongodb/mongo-hadoop/blob/master/core/src/main/java/com/mongodb/hadoop/splitter/SampleSplitter.java
public class CollectionClusteringAnalyzer implements Runnable {
	public static final Document useIdIndexHint = new Document("_id", 1);

	private final long id;
	private final CompletableFuture<CollectionCluster> publishingCf;
	private final long _min;
	private final long _max;
	private final MongoDatabase database;
	private final long averageDocSize;
	private final String collectionName;
	private final ASCIIGUI gui;

	public CollectionClusteringAnalyzer(long id, String collectionName, CompletableFuture<CollectionCluster> publishingCf, long _min, long _max, MongoDatabase database, ASCIIGUI gui, long averageDocSize) {
		this.id = id;
		this.collectionName = collectionName;
		this.publishingCf = publishingCf;
		this._min = _min;
		this._max = _max;
		this.database = database;
		this.gui = gui;
		this.averageDocSize = averageDocSize;
	}

	@Override
	public void run() {
		long docNumber = 0;

		final ObjectId minId = new ObjectId(Long.toHexString(_min) + "0000000000000000");
		final ObjectId maxId = new ObjectId(Long.toHexString(_max) + "0000000000000000");

		try {
			MongoCollection<Document> collection = database.getCollection(collectionName);

			docNumber = collection.countDocuments(Filters.and(
							Filters.gte("_id", minId),
							Filters.lt("_id", maxId))
					, new CountOptions().hint(useIdIndexHint));

			if (docNumber > 0) {
				gui.updateSourceDatabaseDocuments(docNumber, averageDocSize);
			}
		}
		finally {
			publishingCf.complete(new CollectionCluster(docNumber, minId, maxId));
		}
	}
}