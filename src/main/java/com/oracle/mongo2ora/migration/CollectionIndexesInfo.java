package com.oracle.mongo2ora.migration;

import com.mongodb.client.MongoCollection;
import com.oracle.mongo2ora.migration.mongodb.MetadataIndex;
import com.oracle.mongo2ora.migration.mongodb.MongoDBMetadata;
import org.bson.Document;

import java.util.Arrays;

public class CollectionIndexesInfo {
	public int totalMongoDBIndexes;
	public int expectedOracleIndexes;
	public int doneOracleIndexes;
	public String currentOracleIndexName;
	public long currentOracleIndexStartTime = -1;
	public long currentOracleIndexEndTime = -1;

	public CollectionIndexesInfo(MongoCollection<Document> mongoCollection) {
		initialize(mongoCollection);
	}
	private void initialize(MongoCollection<Document> mongoCollection) {
		boolean needSearchIndex = false;
		for (Document i : mongoCollection.listIndexes()) {
			totalMongoDBIndexes++;

			if (i.getString("name").contains("$**") || "text".equals(i.getEmbedded(Arrays.asList("key"), Document.class).getString("_fts"))) {
				needSearchIndex = true; //System.out.println("Need Search Index");
			} else {
				expectedOracleIndexes++;
			}
		}

		if(needSearchIndex) {
			expectedOracleIndexes++;
		}
	}
	public CollectionIndexesInfo(MongoDBMetadata collectionMetadata) {
		initialize(collectionMetadata);
	}
	private void initialize(MongoDBMetadata collectionMetadata) {
		boolean needSearchIndex = false;
		for (MetadataIndex i : collectionMetadata.getIndexes()) {
			totalMongoDBIndexes++;

			if (i.getName().contains("$**") || i.getKey().text) {
				needSearchIndex = true; //System.out.println("Need Search Index");
			} else {
				expectedOracleIndexes++;
			}
		}

		if(needSearchIndex) {
			expectedOracleIndexes++;
		}
	}

	public void initializeIndexStartTime() {
		if(currentOracleIndexStartTime == -1) {
			currentOracleIndexStartTime = System.currentTimeMillis();
		}
	}

	public void startIndex(String indexName) {
		initializeIndexStartTime();
		currentOracleIndexName = indexName;
	}

	public boolean isIndexing() {
		return currentOracleIndexStartTime != -1;
	}

	public void endIndex() {
		doneOracleIndexes++;
		currentOracleIndexEndTime = System.currentTimeMillis();
	}
}
