package com.oracle.mongo2ora.reporting;

public enum IndexType {
	PRIMARY_KEY("Primary Key"),
	JSON_SEARCH("JSON Search (full-text)"),
	MULTIVALUE("Multivalue (array)"),
	COMPOUND("Multiple JSON fields"),
	SIMPLE("Single JSON field"),
	GEO_JSON("Spatial");

	public final String name;

	IndexType(String name) {
		this.name = name;
	}
}
