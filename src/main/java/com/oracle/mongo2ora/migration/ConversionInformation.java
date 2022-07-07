package com.oracle.mongo2ora.migration;

public class ConversionInformation {
	public final long bsonLength;
	public final long osonLength;
	public final long oracleDocs;

	public ConversionInformation(long bsonLength, long osonLength, long oracleDocs) {
		this.bsonLength = bsonLength;
		this.osonLength = osonLength;
		this.oracleDocs = oracleDocs;
	}
}
