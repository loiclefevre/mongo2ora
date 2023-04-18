package com.oracle.mongo2ora.migration.mongodb;

import org.bson.types.ObjectId;

public class DumpCluster {
	public final long count;
	public final long startPosition;

	public DumpCluster(long count, long startPosition) {
		this.count = count;
		this.startPosition = startPosition;
	}
}
