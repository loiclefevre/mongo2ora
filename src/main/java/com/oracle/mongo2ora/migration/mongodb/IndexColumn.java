package com.oracle.mongo2ora.migration.mongodb;

public class IndexColumn {
	public final String name;
	public final boolean asc;

	public IndexColumn(String name, boolean asc) {
		this.name = name;
		this.asc = asc;
	}
}
