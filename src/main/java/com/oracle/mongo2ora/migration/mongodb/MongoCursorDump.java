package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;

public class MongoCursorDump<TResult> implements MongoCursor<TResult> {
	public final FindIterableDump<TResult> findIterable;

	public MongoCursorDump(FindIterableDump<TResult> findIterable) {
		this.findIterable=findIterable;
	}

	@Override
	public void close() {

	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public TResult next() {
		return null;
	}

	@Override
	public int available() {
		return 0;
	}

	@Override
	public TResult tryNext() {
		return null;
	}

	@Override
	public ServerCursor getServerCursor() {
		return null;
	}

	@Override
	public ServerAddress getServerAddress() {
		return null;
	}
}
