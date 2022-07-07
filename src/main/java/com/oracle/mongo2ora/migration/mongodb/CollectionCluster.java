package com.oracle.mongo2ora.migration.mongodb;

import org.bson.types.ObjectId;

public class CollectionCluster {
	public final long count;
	public final ObjectId minId;
	public final ObjectId maxId;

	public CollectionCluster(long count, ObjectId minId, ObjectId maxId) {
		this.count = count;
		this.minId = minId;
		this.maxId = maxId;
	}
}
