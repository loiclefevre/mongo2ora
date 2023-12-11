package com.oracle.mongo2ora.reporting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CollectionReport {
	public final String collectionName;
	public long totalDocumentsLoaded;
	public final boolean sampling;
	public String tableName;
	public boolean mongoDBAPICompatible;
	public boolean wasDropped;

	public final List<IndexReport> indexes = new ArrayList<>();
	public final Map<String,IndexReport> indexesMap = new TreeMap<>();
	public final List<FailedIndexReport> failedIndexes = new ArrayList<>();
	public final Map<String,FailedIndexReport> failedIndexesMap = new TreeMap<>();
	public long totalBSONSize;
	public long totalOSONSize;
	public long tableSize;
	public long totalKeysSize;
	public int compressionLevel;
	public long loadDurationInMS;
	public long totalLoadDurationInMS;

	public CollectionReport(String collectionName, boolean sampling) {
		this.collectionName = collectionName;
		this.sampling = sampling;
	}

	public void addIndex(String name, IndexType type) {
		IndexReport ir = new IndexReport(name, type);
		indexes.add(ir);
		indexesMap.put(name,ir);
	}

	public void replaceIndex(String oldName, String newName) {
		for(IndexReport ir: indexes) {
			if(ir.name.equals(oldName)) {
				ir.name = newName;
				indexesMap.put(newName,ir);
				break;
			}
		}
		indexesMap.remove(oldName);
	}

	public IndexReport getIndex(String name) {
		return indexesMap.get(name);
	}

	public void addFailedIndex(String name, IndexType type) {
		FailedIndexReport fir = new FailedIndexReport(name, type);
		failedIndexes.add(fir);
		failedIndexesMap.put(name,fir);
	}

	public FailedIndexReport getFailedIndex(String name) {
		return failedIndexesMap.get(name);
	}
}
