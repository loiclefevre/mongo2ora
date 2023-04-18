package com.oracle.mongo2ora.migration.mongodb;

import org.bson.types.ObjectId;

public class CollectionCluster {
	public final long count;
	public final boolean sourceDump;
	public ObjectId minId;
	public ObjectId maxId;

	public CollectionCluster(long count, ObjectId minId, ObjectId maxId) {
		this.count = count;
		this.minId = minId;
		this.maxId = maxId;
		this.sourceDump = false;
	}

	public long startPosition;

	public CollectionCluster(long count, long startPosition) {
		this.count = count;
		this.startPosition = startPosition;
		this.sourceDump = true;
	}
}
