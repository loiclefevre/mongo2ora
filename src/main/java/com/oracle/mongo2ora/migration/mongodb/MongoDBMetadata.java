package com.oracle.mongo2ora.migration.mongodb;

public class MongoDBMetadata {
	private MetadataOptions options;
	private MetadataIndex[] indexes;

	private String collectionName;

	public MongoDBMetadata() {
	}

	public MetadataOptions getOptions() {
		return options;
	}

	public void setOptions(MetadataOptions options) {
		this.options = options;
	}

	public MetadataIndex[] getIndexes() {
		return indexes;
	}

	public void setIndexes(MetadataIndex[] indexes) {
		this.indexes = indexes;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
}
