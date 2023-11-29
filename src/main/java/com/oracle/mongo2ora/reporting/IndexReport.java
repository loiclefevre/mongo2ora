package com.oracle.mongo2ora.reporting;

public class IndexReport {
	public String name;

	public final IndexType type;

	public int numberOfFields=1;

	public IndexReport(String name, IndexType type) {
		this.name = name;
		this.type = type;
	}
}
