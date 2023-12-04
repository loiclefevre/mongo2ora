package com.oracle.mongo2ora.reporting;

import com.oracle.mongo2ora.util.Tools;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LoadingReport {
	public final List<CollectionReport> collections = new ArrayList<>();
	public final Map<String,CollectionReport> collectionsMap = new TreeMap<>();

	public String source;
	public int numberOfCollections;
	public int numberOfIndexes;
	public String oracleVersion;
	public int oracleInstanceNumber;
	public String oracleDatabaseType = "Oracle";
	public String sourceDumpFolder;

	public String toCSVString() {
		return String.format(
				"---===[ Migration Report ]===---\n" +
						"\"Source\";\"Date\";\"Collections\";\"Indexes\"\n" +
						"\"%s\";;;",
				source
		);
	}

	public String toString() {
		final StringBuilder r = new StringBuilder();
		r.append(
		String.format(
				"\n-----=====[ Migration Report ]=====-----\n" +
					 "Source ...................... %s\n" +
					 "Destination ................. %s\n" +
					 "Number of collection(s) ..... %d\n" +
					 "Number of index(es) ......... %d\n"
				, source+(sourceDumpFolder.isEmpty()?"":" ("+sourceDumpFolder+")")
				, oracleDatabaseType + " database v" + oracleVersion + (oracleInstanceNumber > 1 ? " (with " + oracleInstanceNumber + " RAC instances)" : "")
				, numberOfCollections
				, numberOfIndexes
		));

		    r.append("\nCollection(s)\n");

		collections.sort(Comparator.comparing(o -> o.collectionName));

		for(CollectionReport cr : collections) {
			r.append("===============================================================================\n");
			r.append("  ").append(cr.collectionName).append('\n');
			r.append("  - Document(s) loaded ...... ").append(cr.totalDocumentsLoaded).append(cr.sampling?" (sampling)":"").append('\n');
			r.append("  - BSON size ............... ").append(Tools.getHumanReadableSize(cr.totalBSONSize)).append('\n');
			r.append("  - OSON size ............... ").append(Tools.getHumanReadableSize(cr.totalOSONSize)).append('\n');
			r.append("  - Table name .............. ").append(cr.tableName).append('\n');
			r.append("  - Table size .............. ").append(Tools.getHumanReadableSize(cr.tableSize)).append('\n');
			if(cr.wasDropped) r.append("  - Was dropped ............. Yes\n");
			r.append("  - MongoDB API compatible .. ").append(cr.mongoDBAPICompatible?"Yes":"No").append('\n');
			r.append("  - Created index(es) ....... ").append(cr.indexes.size()).append('\n');
			if(!cr.failedIndexes.isEmpty()) r.append("  - Failed index(es) ........ ").append(cr.failedIndexes.size()).append('\n');

			int i = 1;
			for(IndexReport ir : cr.indexes) {
				r.append("  - Index #").append(i).append(' ').append(ir.name).append('\n');
				r.append("    - Type .................. ").append(ir.type.name).append('\n');
				if(ir.type == IndexType.COMPOUND || ir.type == IndexType.COMPOUND_MV)
					r.append("    - Fields ................ ").append(ir.numberOfFields).append('\n');
				else
					r.append("    - Fields ................ 1\n");
				r.append("    - Size .................. ").append(Tools.getHumanReadableSize(ir.indexSize)).append('\n');
				if(ir.type == IndexType.COMPOUND_MV || ir.type == IndexType.SIMPLE_MV) {
					r.append("    - Materialized View ..... ").append(ir.materializedViewName).append('\n');
					r.append("        - MV Size ........... ").append(Tools.getHumanReadableSize(ir.materializedViewSize)).append('\n');
				}


				i++;
			}
		}

		return r.toString();
	}

	public void addCollection(String name, boolean sampling) {
		final CollectionReport cr = new CollectionReport(name, sampling);
		collectionsMap.put(name,cr);
		collections.add(cr);
	}

	public CollectionReport getCollection(String collectionName) {
		return collectionsMap.get(collectionName);
	}
}
