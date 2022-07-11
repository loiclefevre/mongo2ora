package com.oracle.mongo2ora.migration;

public class ConversionInformation {
	public long bsonLength;
	public long osonLength;
	public long oracleDocs;
	public Exception exception;

	public ConversionInformation(long bsonLength, long osonLength, long oracleDocs) {
		this.bsonLength = bsonLength;
		this.osonLength = osonLength;
		this.oracleDocs = oracleDocs;
	}

	public ConversionInformation(Exception e) {
		this.exception = e;
	}
}
