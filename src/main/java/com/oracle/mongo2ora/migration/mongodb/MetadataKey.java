package com.oracle.mongo2ora.migration.mongodb;

import java.util.ArrayList;
import java.util.List;

public class MetadataKey {
	public boolean text;
	public final List<IndexColumn> columns = new ArrayList<>();
	public boolean spatial;
	public boolean hashed;

	public void addIndexColumn(String indexColumn, boolean asc) {
		columns.add(new IndexColumn(indexColumn, asc) );
	}

	@Override
	public String toString() {
		return "MetadataKey{" +
				"text=" + text +
				", columns=" + columns +
				", spatial=" + spatial +
				", hashed=" + hashed +
				'}';
	}

	public boolean hasColumns() {
		return !columns.isEmpty();
	}
	public boolean isCompound() {
		return columns.size() > 1;
	}

	public int getNumberOfFields() {
		return columns.size();
	}
}
