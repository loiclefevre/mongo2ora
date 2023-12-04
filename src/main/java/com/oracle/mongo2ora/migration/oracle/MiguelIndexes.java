package com.oracle.mongo2ora.migration.oracle;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.Configuration;
import com.oracle.mongo2ora.migration.mongodb.IndexColumn;
import com.oracle.mongo2ora.migration.mongodb.MetadataIndex;
import com.oracle.mongo2ora.migration.mongodb.MetadataKey;
import com.oracle.mongo2ora.reporting.IndexReport;
import com.oracle.mongo2ora.reporting.IndexType;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.oracle.mongo2ora.Main.MAX_STRING_SIZE;
import static com.oracle.mongo2ora.Main.REPORT;
import static com.oracle.mongo2ora.migration.oracle.MiguelIndexes.PathType.ARRAY;
import static com.oracle.mongo2ora.migration.oracle.MiguelIndexes.PathType.OTHER;

public class MiguelIndexes {
	private static final Logger LOGGER = Loggers.getLogger("Miguel indexes");

	// source: https://github.com/oracle/json-in-db/blob/master/MigrationTools/IndexParser/ora_idx_parser.body.sql
	public static void build(OracleCollectionInfo oracleCollectionInfo, Connection c, Configuration conf, MetadataIndex[] indexes, int oracleVersion, ASCIIGUI gui) {
		String jsonDataGuide;

		final Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo = new TreeMap<>();

		try {
			try (Statement s = c.createStatement()) {
				final long rows = getNumberOfRows(oracleCollectionInfo, s);

				computeFieldsInfoFromDataGuide(oracleCollectionInfo, conf, oracleVersion, rows, fieldsInfo, s);

				// stores the types per JSON Path *found* inside the collection by JSON_DATAGUIDE()
				final Map<String, String> keyTypes = prepareKeyTypes(fieldsInfo);

				final Map<String, String> keyFieldInfoPathMap = prepareKeyFieldInfoPathMap(fieldsInfo);

				// stores the types per JSON Path *found* inside the collection by JSON_DATAGUIDE()
				final Map<String, PathType> allMatPaths = prepareAllMatPaths(fieldsInfo, keyTypes);

				for (MetadataIndex mi : indexes) {
					if ("_id_".equals(mi.getName())) {
						LOGGER.info("Primary key index " + mi.getName() + " already created.");

						for (IndexReport ir : REPORT.getCollection(oracleCollectionInfo.getCollectionName()).indexes) {
							if (ir.type == IndexType.PRIMARY_KEY) {

								s.execute("alter index " + ir.name + " rename to \"" + oracleCollectionInfo.getTableName() + "$" + mi.getName() + "\"");
								REPORT.getCollection(oracleCollectionInfo.getCollectionName()).replaceIndex(ir.name, oracleCollectionInfo.getTableName() + "$" + mi.getName() );
								break;
							}
						}

						continue;
					}
					else if (!mi.getKey().hasColumns()) {
						gui.startIndex(mi.getName());
						LOGGER.info("Index " + mi.getName() + " (used for sharded configuration) has no field!");
						gui.endIndex(mi.getName(), false);
						REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addFailedIndex(mi.getName(), IndexType.SHARDED );
						continue;
					}

					// LOGGER.info("Now creating index " + oracleCollectionInfo.getTableName() + "$" + mi.getName());

					boolean isMaterializedViewBasedIndex = false;
					boolean noParallelArrays = !hasParallelArrays(keyTypes, mi.getKey());

					boolean allFieldTypesKnown = true;
					boolean noMultiTypeField = true;
					boolean noObjectTypeField = true;
					boolean noOrphanArrayTypeField = true;
					boolean rightMaxStringSizeForMVBasedIndex = true;

					if (noParallelArrays) {
						for (IndexColumn ic : mi.getKey().columns) {
							// Controls per field
							allFieldTypesKnown &= keyTypes.containsKey(ic.name);
							final String type = keyTypes.get(ic.name);
							noMultiTypeField &= !"multitype".equals(type);
							noObjectTypeField &= !"object".equals(type);
							if ("array".equals(type) && !keyTypes.containsKey(ic.name + "[*]")) {
								noOrphanArrayTypeField = false;
							}

							// Logging
							//LOGGER.info("- field " + ic.name + " " + (ic.asc ? "asc" : "desc") + " " + keyTypes.containsKey(ic.name) + " " + keyTypes.get(ic.name));

							if (allFieldTypesKnown && noMultiTypeField && noObjectTypeField && noOrphanArrayTypeField) {
								if (allMatPaths.containsKey(ic.name) && allMatPaths.get(ic.name) == ARRAY) {
									isMaterializedViewBasedIndex = true;

									if (oracleVersion >= 23) {
										if (!"EXTENDED".equals(MAX_STRING_SIZE)) {
											rightMaxStringSizeForMVBasedIndex = false;
										}
									}
								}
							}
						}
					}


					if (rightMaxStringSizeForMVBasedIndex && noParallelArrays && allFieldTypesKnown && noMultiTypeField && noObjectTypeField && noOrphanArrayTypeField) {
						if (mi.isTtl()) {
							// TODO
							//LOGGER.info("Create TTL index ...");
							LOGGER.warn("/!\\ TTL index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + ", this feature is coming soon in mongo2ora..");
						}
						else if (isMaterializedViewBasedIndex) {
							LOGGER.info("Create materialized view based index ...");
							boolean mvCreatedWithSuccess = buildMaterializedView(gui, s, oracleCollectionInfo, mi, keyFieldInfoPathMap, conf, oracleVersion, keyTypes, fieldsInfo);
							if(mvCreatedWithSuccess) {
								createIndexOnMaterializedView(gui, s, oracleCollectionInfo, mi, keyFieldInfoPathMap, conf, oracleVersion, keyTypes, fieldsInfo);
							}
						}
						else {
							//LOGGER.info("Create index ...");
							createRegularIndex(gui, s, oracleCollectionInfo, mi, keyFieldInfoPathMap, conf, oracleVersion, keyTypes);
						}
					}
					else {
						gui.startIndex(mi.getName());

						if (!allFieldTypesKnown) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since we don't know about the type of some field(s)");
						}

						if (!noMultiTypeField) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since some field(s) have multiple types.");
						}

						if (!noObjectTypeField) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since some field(s) have an object type.");
						}

						if (!noOrphanArrayTypeField) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since some field(s) have an array type but no documents were found matching this.");
						}

						if (!noParallelArrays) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since at least 2 paths have distinct parallel arrays!");
						}

						if (!rightMaxStringSizeForMVBasedIndex) {
							LOGGER.warn("/!\\ Index " + mi.getName() + " will NOT be created on collection " + oracleCollectionInfo.getCollectionName() + " since it requires a materialized view that can't be created because max_string_size parameter value is not EXTENDED!");
						}

						gui.endIndex(mi.getName(), false);
						REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addFailedIndex(mi.getName(), mi.getKey().columns.size() > 1 ? IndexType.COMPOUND : IndexType.SIMPLE);
					}
				}
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void createIndexOnMaterializedView(ASCIIGUI gui, Statement stmt, OracleCollectionInfo oracleCollectionInfo, MetadataIndex mi, Map<String, String> keyFieldInfoPathMap, Configuration conf, int oracleVersion, Map<String, String> keyTypes, Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo) {
		final StringBuilder s = new StringBuilder();

		s.append("create ").append(mi.isUnique() ? "unique " : "").append("index ").append(oracleVersion >= 23 ? "if not exists " : "").append("\"").append(oracleCollectionInfo.getTableName()).append('$').append(mi.getName()).append("\" on ")
				.append("mv4qrw_").append(oracleCollectionInfo.getTableName()).append('$').append(mi.getName()).append(" ( ");

		int fields = 0;
		for (IndexColumn ic : mi.getKey().columns) {
			if (fields > 0) {
				s.append(", ");
			}

			s.append("\"").append(ic.name).append("\" ").append(ic.asc ? "asc" : "desc");

			fields++;
		}

		s.append(")").append(" PARALLEL").append(conf.maxSQLParallelDegree == -1 ? "" : " " + conf.maxSQLParallelDegree).append(" compute statistics");

		LOGGER.info(s.toString());

		try {
			// TODO compress prefix column?
			// TODO advanced compress?
			// TODO nologging?
			// TODO drop before creating it (--drop-index)

			stmt.execute(s.toString());
			stmt.execute("alter session disable parallel ddl");

			gui.endIndex(mi.getName(), true);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName()), mi.getKey().columns.size() > 1 ? IndexType.COMPOUND_MV : IndexType.SIMPLE_MV);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).numberOfFields = mi.getKey().getNumberOfFields();
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).materializedViewName = "mv4qrw_"+oracleCollectionInfo.getTableName()+ "$"+ mi.getName();

			// TODO disable parallel index?
		}
		catch (SQLException sqle) {
			LOGGER.error("Creating index on materialized view! ", sqle);

			gui.endIndex(mi.getName(), false);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName()), mi.getKey().getNumberOfFields() > 1 ? IndexType.COMPOUND_MV : IndexType.SIMPLE_MV);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).numberOfFields = mi.getKey().getNumberOfFields();
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).materializedViewName = "mv4qrw_"+oracleCollectionInfo.getTableName()+ "$"+ mi.getName();
		}
	}


	private static Map<String, String> prepareKeyFieldInfoPathMap(Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo) {
		final Map<String, String> keyFieldInfoPathMap = new HashMap<>();

		for (OracleCollectionInfo.FieldInfo fi : fieldsInfo.values()) {
			if ("$".equals(fi.path) || "$._id".equals(fi.path))
				continue;

			final String key = fi.path.substring(2).replaceAll("\\\"", "");
			keyFieldInfoPathMap.put(key, fi.path);
		}

		return keyFieldInfoPathMap;
	}

	private static boolean buildMaterializedView(ASCIIGUI gui, Statement stmt, OracleCollectionInfo oracleCollectionInfo, MetadataIndex mi, Map<String, String> keyFieldInfoPathMap, Configuration conf, int oracleVersion, Map<String, String> keyTypes, Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo) {
		final StringBuilder s = new StringBuilder();

		s.append("create materialized view ").append(oracleVersion >= 23 ? "if not exists " : "").append("mv4qrw_").append(oracleCollectionInfo.getTableName()).append('$').append(mi.getName()).append("\n")
				.append("""
						build immediate
						refresh fast on statement with primary key
						as
						select col.id, jt.*
						  from \"""").append(oracleCollectionInfo.getTableName()).append("\" col,\n")
				.append("       json_table( col.").append(conf.mongodbAPICompatible || oracleVersion >= 23 ? "DATA" : "JSON_DOCUMENT").append(", ")
				.append(getMVJSONPathsRecursive(mi, keyTypes, keyFieldInfoPathMap, fieldsInfo))
				.append("\n                 ) jt");

		LOGGER.debug(s.toString());

		try {
			// TODO compress?
			// TODO advanced compress?
			// TODO nologging?
			// TODO drop before creating it (--drop-index)

			gui.startIndex(mi.getName());
			stmt.execute("alter session enable parallel ddl");
			try {
				stmt.execute("drop materialized view "+"mv4qrw_"+oracleCollectionInfo.getTableName()+"$"+mi.getName());
			}catch(SQLException ignored) {}
			stmt.execute(s.toString());

			return true;
		}
		catch (SQLException sqle) {
			LOGGER.error("Creating materialized view for index! ", sqle);
			gui.endIndex(mi.getName(), false);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName()), mi.getKey().getNumberOfFields() > 1 ? IndexType.COMPOUND : IndexType.SIMPLE);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).numberOfFields = mi.getKey().getNumberOfFields();
		}

		return false;
	}

	private static String getMVJSONPathsRecursive(MetadataIndex mi, Map<String, String> keyTypes, Map<String, String> keyFieldInfoPathMap, Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo) {
		final StringBuilder s = new StringBuilder();

		RecursiveMV rmv = new RecursiveMV(mi.getKey().columns, keyTypes);

		s.append(rmv.getJSONTableFieldsMap(keyFieldInfoPathMap));

		return s.toString();
	}

	private static void getMVJSONPathsRecursiveArray(StringBuilder s, int level, String rootPath, String remainingSubPath, Map<String, String> keyTypes, Map<String, String> keyFieldInfoPathMap) {
		final JSONPathWalker pathWalker = new JSONPathWalker(remainingSubPath);

		while (pathWalker.hasNextPath()) {
			final String path = pathWalker.nextPath();

			LOGGER.info("MV: path=" + path);

			switch (keyTypes.get(rootPath + "." + path)) {
				case "object":
					if (pathWalker.hasNextPath()) continue;
					LOGGER.error("Indexed field is an object!");
					break;
				case "array":
					LOGGER.warn("Detected array in JSON Path => recursive call...");
					getMVJSONPathsRecursiveArray(s, level + 1, path, pathWalker.getRemainingSubPath(), keyTypes, keyFieldInfoPathMap);
					break;
				default:
					if (!s.isEmpty()) {
						s.append(",\n");
					}
					s.append("\"").append(rootPath).append("\"").append(" <type> path '").append(keyFieldInfoPathMap.get(rootPath + "." + path)).append("'");
					break;
			}
		}

	}

	private static void createRegularIndex(ASCIIGUI gui, Statement stmt, OracleCollectionInfo oracleCollectionInfo, MetadataIndex mi, Map<String, String> keyFieldInfoPathMap, Configuration conf, int oracleVersion, Map<String, String> keyTypes) {
		final StringBuilder s = new StringBuilder();

		s.append("create ").append(mi.isUnique() ? "unique " : "").append("index ").append(oracleVersion >= 23 ? "if not exists " : "").append("\"").append(oracleCollectionInfo.getTableName()).append('$').append(mi.getName()).append("\" on \"").append(oracleCollectionInfo.getTableName()).append("\" ( ");

		int fields = 0;
		for (IndexColumn ic : mi.getKey().columns) {
			if (fields > 0) {
				s.append(", ");
			}
			s.append("json_value( ").append(conf.mongodbAPICompatible || oracleVersion >= 23 ? "DATA" : "JSON_DOCUMENT").append(", '").append(keyFieldInfoPathMap.get(ic.name));

			switch (keyTypes.get(ic.name)) {
				case "string":
					s.append(".stringOnly()");
					break;
				case "number":
				case "double":
					s.append(".numberOnly()");
					break;
				case "timestamp":
					s.append(".dateTimeOnly()");
					break;
				case "boolean":
					s.append(".booleanOnly()");
					break;
				case "binary":
					s.append(".binaryOnly()");
					break;

				default:
					LOGGER.error("Type " + keyTypes.get(ic.name) + " NOT supported!");
			}

			s.append("' error on error null on empty) ").append(ic.asc ? "asc" : "desc");

			fields++;
		}

		s.append(", 1)").append(" PARALLEL").append(conf.maxSQLParallelDegree == -1 ? "" : " " + conf.maxSQLParallelDegree).append(" compute statistics");

		LOGGER.info(s.toString());

		try {
			// TODO compress prefix column?
			// TODO advanced compress?
			// TODO nologging?
			// TODO drop before creating it (--drop-index)

			gui.startIndex(mi.getName());
			stmt.execute("alter session enable parallel ddl");
			stmt.execute(s.toString());
			stmt.execute("alter session disable parallel ddl");
			gui.endIndex(mi.getName(), true);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName()), mi.getKey().columns.size() > 1 ? IndexType.COMPOUND : IndexType.SIMPLE);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).numberOfFields = mi.getKey().getNumberOfFields();

			// TODO disable parallel index?
		}
		catch (SQLException sqle) {
			LOGGER.error("Creating index! ", sqle);
			gui.endIndex(mi.getName(), false);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).addFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName()), mi.getKey().getNumberOfFields() > 1 ? IndexType.COMPOUND : IndexType.SIMPLE);
			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).getFailedIndex(String.format("%s$%s", oracleCollectionInfo.getTableName(), mi.getName())).numberOfFields = mi.getKey().getNumberOfFields();
		}
	}

	private static boolean hasParallelArrays(Map<String, String> keyTypes, MetadataKey key) {

		boolean otherBranch;
		List<String> prefix = new ArrayList<>();

		for (IndexColumn c : key.columns) {
			otherBranch = false;
			final List<String> tmpPrefix = new ArrayList<>();
			int idxCursor = 0;

			final JSONPathWalker pathWalker = new JSONPathWalker(c.name);

			while (pathWalker.hasNextPath()) {
				final String path = pathWalker.nextPath();
				tmpPrefix.add(pathWalker.getLastPathPart());

				if (prefix.isEmpty() || prefix.size() < idxCursor) {
					if (keyTypes.containsKey(path) && "array".equals(keyTypes.get(path))) {
						if (otherBranch) {
							return true;
						}
						prefix = tmpPrefix;
					}
				}
				else {
					if (!prefix.get(idxCursor).equals(pathWalker.getLastPathPart())) {
						otherBranch = true;
					}

					if (otherBranch && "array".equals(keyTypes.get(path))) {
						return true;
					}
				}

				idxCursor++;
			}
		}

		return false;
	}

	enum PathType {
		ARRAY,
		OTHER;
	}

	private static Map<String, PathType> prepareAllMatPaths(Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo,
															Map<String, String> keyTypes) {
		//LOGGER.info("--- prepareAllMatPaths");

		final Map<String, PathType> allMatPaths = new HashMap<>();

		boolean includeInMatV = false;

		for (OracleCollectionInfo.FieldInfo fi : fieldsInfo.values()) {
			if ("$".equals(fi.path) || "$._id".equals(fi.path))
				continue;

			final String key = fi.path.substring(2).replaceAll("\\\"", "");

			final JSONPathWalker pathWalker = new JSONPathWalker(key);

			while (pathWalker.hasNextPath()) {
				final String path = pathWalker.nextPath();

				//LOGGER.info("Checking "+path);

				if (keyTypes.containsKey(path) && "array".equals(keyTypes.get(path))) {
					//LOGGER.info(path+ " is an array, we'll create a materialized view!");
					allMatPaths.put(key, ARRAY);
					includeInMatV = true;
					break;
				}
			}
		}

		if (includeInMatV) {
			for (OracleCollectionInfo.FieldInfo fi : fieldsInfo.values()) {
				if ("$".equals(fi.path) || "$._id".equals(fi.path))
					continue;

				final String key = fi.path.substring(2).replaceAll("\\\"", "");

				if (!allMatPaths.containsKey(key)) {
					allMatPaths.put(key, OTHER);
				}
			}
		}

		return allMatPaths;
	}

	private static Map<String, String> prepareKeyTypes(Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo) {
		//LOGGER.info("--- prepareKeyTypes");
		final Map<String, String> keyTypes = new HashMap<>();

		for (OracleCollectionInfo.FieldInfo fi : fieldsInfo.values()) {
			if ("$".equals(fi.path))
				continue;

			final String key = "$._id".equals(fi.path) ? "_id" : fi.path.substring(2).replaceAll("\\\"", "");
			if (keyTypes.containsKey(key)) {
				keyTypes.put(key, "multitype");
				LOGGER.warn("Field " + key + " has multiple types!");
			}
			else {
				keyTypes.put(key, fi.type);
			}
		}

		return keyTypes;
	}

	private static long getNumberOfRows(OracleCollectionInfo oracleCollectionInfo, Statement s) throws SQLException {
		try (ResultSet r = s.executeQuery("select count(1) from \"" + oracleCollectionInfo.getTableName() + "\"")) {
			if (r.next()) {
				return r.getLong(1);
			}
			else {
				throw new RuntimeException("Unable to retrieve number of documents from collection " + oracleCollectionInfo.getCollectionName());
			}
		}
	}

	private static void computeFieldsInfoFromDataGuide(OracleCollectionInfo oracleCollectionInfo, Configuration conf, int oracleVersion, long rows, Map<String, OracleCollectionInfo.FieldInfo> fieldsInfo, Statement s) throws SQLException {
		final File dataGuideFile = new File(conf.sourceDumpFolder, oracleCollectionInfo.getCollectionName() + ".dataguide." + rows + ".json");

		LOGGER.warn("Now building indexes, the larger the collection, the longer the process!");
		if (dataGuideFile.exists()) {
			LOGGER.info("Found existing Data Guide for " + oracleCollectionInfo.getCollectionName() + " with matching number of JSON documents (" + rows + "), reloading it!");

			final ObjectMapper mapper = new ObjectMapper();
			try {
				final OracleCollectionInfo.FieldInfo[] fis = mapper.readValue(dataGuideFile, OracleCollectionInfo.FieldInfo[].class);
				LOGGER.info("Data Guide with " + fis.length + " field(s) information reloaded!");

				for (OracleCollectionInfo.FieldInfo fi : fis) {
					fieldsInfo.put(fi.path, fi);
				}
			}
			catch (StreamReadException e) {
				throw new RuntimeException(e);
			}
			catch (DatabindException e) {
				throw new RuntimeException(e);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			LOGGER.info("Gathering Data Guide from " + oracleCollectionInfo.getCollectionName() + " on " + rows + " JSON documents...");
			try (ResultSet r = s.executeQuery("with dg as (select json_object( 'dg' : json_dataguide( " + (conf.mongodbAPICompatible || oracleVersion >= 23 ? "DATA" : "JSON_DOCUMENT") + ", dbms_json.format_flat, DBMS_JSON.GEOJSON/*+DBMS_JSON.GATHER_STATS*/) format JSON returning clob) as json_document from \"" + oracleCollectionInfo.getTableName() + "\" /*where rownum <= 1000*/)\n" +
					"select u.field_path, type, length/*, low, high*/ from dg nested json_document columns ( nested dg[*] columns (field_path path '$.\"o:path\"', type path '$.type', length path '$.\"o:length\"'/*, low path '$.\"o:low_value\"', high path '$.\"o:high_value\"'*/ )) u")) {
				while (r.next()) {
					String path = r.getString(1);
					if (!r.wasNull()) {
						fieldsInfo.put(path, new OracleCollectionInfo.FieldInfo(path, r.getString(2), r.getInt(3)));
					}
				}
			}

			// persist the data guide for ROWS rows...
			try (PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(dataGuideFile), 16 * 1024))) {
				out.println("[");
				int i = 0;
				for (String path : fieldsInfo.keySet()) {
					final OracleCollectionInfo.FieldInfo fi = fieldsInfo.get(path);
					if (i > 0) out.println(",");
					out.print(String.format("{\"path\": \"%s\",\"type\": \"%s\",\"length\": %d}", fi.path.replaceAll("\"", "\\\\\""), fi.type, fi.length));
					i++;
				}
				out.println("\n]");
			}
			catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	static class JSONPathWalker {
		private final String jsonPath;
		private final String[] allPathParts;
		private final StringBuilder currentPath;

		private int idx;
		private final int totalParts;

		JSONPathWalker(String jsonPath) {
			this.jsonPath = jsonPath;
			this.allPathParts = jsonPath.split("\\.");
			totalParts = allPathParts.length;
			currentPath = new StringBuilder();
		}

		public String getLastPathPart() {
			return allPathParts[idx - 1];
		}

		public boolean hasNextPath() {
			return idx < totalParts;
		}

		public String nextPath() {
			if (!currentPath.isEmpty()) {
				currentPath.append('.');
			}
			currentPath.append(allPathParts[idx++]);
			return currentPath.toString();
		}

		public String getRemainingSubPath() {
			final StringBuilder s = new StringBuilder();

			for (int i = idx; i < totalParts; i++) {
				if (s.length() > 0) {
					s.append('.');
				}
				s.append(allPathParts[i]);
			}

			return s.toString();
		}
	}

	private static class RecursivePart {

		private final String jsonPath;
		private String parentJSONPath;
		private final String type;

		private final List<RecursivePart> children = new ArrayList<>();
		private final Map<String, RecursivePart> childrenMap = new HashMap<>();
		private final int level;

		public RecursivePart(String jsonPath, String type, int level) {
			this.jsonPath = jsonPath;
			this.type = type;
			this.level = level;
		}

		public RecursivePart(String parentJSONPath, String jsonPath, String type, int level) {
			this.parentJSONPath = parentJSONPath;
			this.jsonPath = jsonPath;
			this.type = type;
			this.level = level;
		}

		public void initialize(List<IndexColumn> columns, Map<String, String> keyTypes) {
			for (IndexColumn ic : columns) {
				if ("array".equals(type)) {
					// skip columns that are not children of the root jsonPath
					//LOGGER.info("in array subpath:" + ic.name + ", is it sub-path of " + jsonPath + " ?");
					if (ic.name.startsWith(jsonPath)) {
						final JSONPathWalker pathWalker = new JSONPathWalker(ic.name.substring(jsonPath.length() + 1));

						//LOGGER.info("- child sub-path: " + ic.name.substring(jsonPath.length() + 1));

						boolean done = false;
						while (pathWalker.hasNextPath() && !done) {
							final String path = pathWalker.nextPath();

							//LOGGER.info("MV: path(" + level + ")=" + jsonPath + "." + path);

							switch (keyTypes.get(jsonPath + "." + path)) {
								case "object":
									if (pathWalker.hasNextPath()) continue;
									LOGGER.error("Indexed field is an object!");
									return;

								case "array":
									//LOGGER.warn("Detected array in JSON Path => recursive call...");
									if (!childrenMap.containsKey(path)) {
										RecursivePart array = new RecursivePart(path, "array", level + 1);
										array.initialize(columns, keyTypes);
										children.add(array);
										childrenMap.put(path, array);
									}
									done = true;
									break;

								default:
									RecursivePart leaf = new RecursivePart(jsonPath, path, keyTypes.get(jsonPath + "." + path), level + 1);
									children.add(leaf);
									break;
							}
						}
					}
				}
				else {
					final JSONPathWalker pathWalker = new JSONPathWalker(ic.name);

					boolean done = false;
					while (pathWalker.hasNextPath() && !done) {
						final String path = pathWalker.nextPath();

						//LOGGER.info("MV: path(" + level + ")=" + path);

						switch (keyTypes.get(path)) {
							case "object":
								if (pathWalker.hasNextPath()) continue;
								LOGGER.error("Indexed field is an object!");
								return;

							case "array":
								//LOGGER.warn("Detected array in JSON Path => recursive call...");
								if (!childrenMap.containsKey(path)) {
									RecursivePart array = new RecursivePart(path, "array", level + 1);
									array.initialize(columns, keyTypes);
									children.add(array);
									childrenMap.put(path, array);
								}
								done = true;
								break;

							default:
								RecursivePart leaf = new RecursivePart(path, keyTypes.get(path), level + 1);
								children.add(leaf);
								break;
						}
					}
				}
			}
		}

		public String dumpHierarchy() {
			final StringBuilder s = new StringBuilder(jsonPath + "(" + type + ", " + level + ")");

			for (RecursivePart c : children) {
				s.append("\n");
				s.append(" ".repeat(Math.max(0, 4 * level)));
				s.append("- ").append(c.dumpHierarchy());
			}

			return s.toString();
		}

		public String visitChildren(Map<String, String> keyFieldInfoPathMap) {
			final StringBuilder s = new StringBuilder();
			if ("$".equals(jsonPath)) {
				s.append("\n").append(" ".repeat(19 + 4 * level)).append("'$' error on error null on empty columns (\n");
			}
			else if ("array".equals(type)) {
				s.append(" ".repeat(19 + 4 * level)).append("nested path '").append(keyFieldInfoPathMap.get(jsonPath)).append("[*]' columns (\n");
			}
			else {
				s.append(" ".repeat(19 + 4 * level)).append("\"").append(parentJSONPath != null ? parentJSONPath + "." : "").append(jsonPath).append("\"").append(" ").append(getSQLType(type)).append(" path '");

				if (parentJSONPath != null) {
					final String realPath = keyFieldInfoPathMap.get(parentJSONPath + "." + jsonPath);
					s.append(realPath.replace(parentJSONPath + ".", ""));
				}
				else {
					s.append(keyFieldInfoPathMap.get(jsonPath));
				}
				s.append("'");
			}

			int i = 0;
			for (RecursivePart c : children) {
				if (i > 0) s.append(",\n");

				s.append(c.visitChildren(keyFieldInfoPathMap));

				i++;
			}


			if ("$".equals(jsonPath) || "array".equals(type)) {
				s.append("\n").append(" ".repeat(19 + 4 * level)).append(')');
			}


			return s.toString();
		}

		private String getSQLType(String type) {
			switch (type) {
				case "string":
					return "varchar2";
				case "timestamp":
					return "timestamp";
				case "number":
				case "double":
					return "number";
				// TODO boolean
			}

			return "error";
		}
	}

	private static class RecursiveMV {

		RecursivePart root;

		public RecursiveMV(List<IndexColumn> columns, Map<String, String> keyTypes) {
			root = new RecursivePart("$", "object", 0);
			root.initialize(columns, keyTypes);

			// LOGGER.info(root.dumpHierarchy());
		}

		public String getJSONTableFieldsMap(Map<String, String> keyFieldInfoPathMap) {
			return root.visitChildren(keyFieldInfoPathMap);
		}
	}
}
