package com.oracle.mongo2ora.reporting;

public enum IndexType {
	PRIMARY_KEY("Primary Key"),
	JSON_SEARCH("JSON Search (full-text)"),
	MULTIVALUE("Multivalue (array)"),
	COMPOUND("Multiple JSON fields"),
	COMPOUND_MV("Multiple JSON fields (Materialized View based)"),
	SIMPLE("Single JSON field"),
	SIMPLE_MV("Single JSON field (Materialized View based)"),
	GEO_JSON("Spatial"),
	SHARDED("Sharded");

	public final String name;

	IndexType(String name) {
		this.name = name;
	}
}
