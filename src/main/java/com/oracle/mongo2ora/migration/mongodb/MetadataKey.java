package com.oracle.mongo2ora.migration.mongodb;

import java.util.ArrayList;
import java.util.List;

public class MetadataKey {
	public boolean text;
	public final List<IndexColumn> columns = new ArrayList<>();
	public boolean spatial;

	public void addIndexColumn(String indexColumn, boolean asc) {
		columns.add(new IndexColumn(indexColumn, asc) );
	}
}
