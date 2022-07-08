package com.oracle.mongo2ora.migration;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;

public class CollectionIndexesInfo {
	private int totalMongoDBIndexes;
	private int expectedOracleIndexes;

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

}
