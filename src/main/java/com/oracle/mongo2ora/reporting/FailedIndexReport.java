package com.oracle.mongo2ora.reporting;

public class FailedIndexReport {
	public String name;

	public final IndexType type;

	public int numberOfFields=1;

	public FailedIndexReport(String name, IndexType type) {
		this.name = name;
		this.type = type;
	}
}
