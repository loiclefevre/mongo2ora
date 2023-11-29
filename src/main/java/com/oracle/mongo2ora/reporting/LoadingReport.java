package com.oracle.mongo2ora.reporting;

import java.util.ArrayList;
import java.util.List;

public class LoadingReport {
	public final List<CollectionReport> collections = new ArrayList<>();

	public String source;
	public int numberOfCollections;
	public int numberOfIndexes;

	public String toCSVString() {
		return String.format(
				"---===[ Migration Report ]===---\n" +
				"\"Source\";\"Date\";\"Collections\";\"Indexes\"\n"+
						"\"%s\";;;",
				source
		);
	}

	public String toString() {
		return String.format(
				"\n-----=====[ Migration Report ]=====-----\n" +
				"- Source: ................ %s\n"+
				"- Number of collection(s): %d\n"+
				"- Number of index(es): ... %d\n"
				, source
				, numberOfCollections
				, numberOfIndexes
		);
	}
}
