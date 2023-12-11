package com.oracle.mongo2ora.migration.oracle;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.Configuration;
import com.oracle.mongo2ora.migration.mongodb.IndexColumn;
import com.oracle.mongo2ora.migration.mongodb.MetadataIndex;
import com.oracle.mongo2ora.migration.mongodb.MetadataKey;
import com.oracle.mongo2ora.migration.mongodb.MongoDBMetadata;
import com.oracle.mongo2ora.reporting.IndexReport;
import com.oracle.mongo2ora.reporting.IndexType;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.Document;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static com.oracle.mongo2ora.Main.REPORT;
import static com.oracle.mongo2ora.util.Tools.getDurationSince;

public class OracleCollectionInfo {
	private static final Logger LOGGER = Loggers.getLogger("index");

	private final String user;
	private final String collectionName;
	private final boolean autonomousDatabase;

	private String tableName;
	private boolean mixedCase;
	public String isJsonConstraintName;
	private boolean isJsonConstraintEnabled;
	private String isJsonConstraintText;
	private boolean isJsonConstraintIsOSONFormat;

	private String primaryKeyIndexName;
	private String primaryKeyIndexStatus;
	public boolean emptyDestinationCollection;
	//public boolean needSearchIndex;

	public OracleCollectionInfo(String user, String collectionName, boolean autonomousDatabase) {
		this.user = user;
		this.collectionName = collectionName;
		//this.tableName = computeProperTableName(collectionName);
		this.autonomousDatabase = autonomousDatabase;
	}

	public void retrieveTableName(Connection c) throws SQLException {
		try (PreparedStatement p = c.prepareStatement("select u.json_descriptor.tableName.string() from user_soda_collections u where uri_name=?")) {
			p.setString(1, collectionName);
			try (ResultSet r = p.executeQuery()) {
				if (r.next()) {
					this.tableName = r.getString(1);
					REPORT.getCollection(collectionName).tableName = this.tableName;
				}
			}
		}
	}

	private static String computeProperTableName(String collectionName) {
		boolean lowerCaseChar = false;
		boolean upperCaseChar = false;

		for (int i = 0; i < collectionName.length(); i++) {
			final char c = collectionName.charAt(i);
			if (Character.isLowerCase(c)) {
				lowerCaseChar = true;
			}
			else if (Character.isUpperCase(c)) {
				upperCaseChar = true;
			}
		}

		if ((lowerCaseChar && !upperCaseChar) || (!lowerCaseChar && upperCaseChar)) {
			return collectionName.toUpperCase();
		}
		else {
			//mixedCase = true;
			return collectionName;
		}
	}

	public static OracleCollectionInfo getCollectionInfoAndPrepareIt(PoolDataSource pds, PoolDataSource adminPDS, Configuration conf, String collectionName, boolean autonomousDatabase)
			throws SQLException, OracleException {
		final String user = conf.destinationUsername.toUpperCase();
		final boolean dropAlreadyExistingCollection = conf.dropAlreadyExistingCollection;
		final boolean mongoDBAPICompatible = conf.mongodbAPICompatible;
		final boolean forceOSON = conf.forceOSON;
		final boolean buildSecondaryIndexes = conf.buildSecondaryIndexes;
		final Properties collectionsProperties = conf.collectionsProperties;

		final OracleCollectionInfo ret = new OracleCollectionInfo(user, collectionName, autonomousDatabase);

		try (Connection c = adminPDS.getConnection()) {
			try (Connection userConnection = pds.getConnection()) {
				// SODA: ensure the collection do exists!
				final Properties props = new Properties();
				props.put("oracle.soda.sharedMetadataCache", "true");
				props.put("oracle.soda.localMetadataCache", "true");

				final OracleRDBMSClient cl = new OracleRDBMSClient(props);
				final OracleDatabase db = cl.getDatabase(userConnection);
				OracleCollection sodaCollection = db.openCollection(ret.collectionName);

				REPORT.getCollection(collectionName).mongoDBAPICompatible = mongoDBAPICompatible;

				ret.emptyDestinationCollection = true;

				if (sodaCollection == null) {
					LOGGER.info((mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection does not exist => creating it");
					sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection(db, ret.collectionName, forceOSON, collectionsProperties) : createClassicCollection(db, ret.collectionName, forceOSON);
					if (sodaCollection == null) {
						throw new IllegalStateException("Can't create " + (mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection: " + ret.collectionName);
					}
					else {
						ret.retrieveTableName(userConnection);
					}
				}
				else {
					ret.retrieveTableName(userConnection);

					try (Statement s = userConnection.createStatement()) {
						try (ResultSet r = s.executeQuery("select count(1) from \"" + ret.tableName + "\" where rownum = 1")) {
							if (r.next() && r.getInt(1) == 1) {
								// THERE IS AT LEAST ONE ROW!
								if (dropAlreadyExistingCollection) {
									LOGGER.warn((mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection does exist => dropping it (requested with --drop CLI argument)");
									sodaCollection.admin().drop();
									REPORT.getCollection(collectionName).wasDropped = true;
									LOGGER.info((mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection does exist => re-creating it");
									// TODO manage contentColumn data type:
									// TODO 21c+ => JSON
									// TODO 19c => BLOB if not autonomous database and not mongodb api compatible
									// TODO 19c => BLOB OSON if autonomous database or not mongodb api compatible
									sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection(db, ret.collectionName, forceOSON, collectionsProperties) : createClassicCollection(db, ret.collectionName, forceOSON);
									if (sodaCollection == null) {
										throw new IllegalStateException("Can't re-create " + (mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection: " + ret.collectionName);
									}
								}
								else {
									// avoid migrating data into non empty destination collection (no conflict to manage)
									ret.emptyDestinationCollection = buildSecondaryIndexes;
									return ret;
								}
							}
							else {
								if (dropAlreadyExistingCollection) {
									LOGGER.warn((mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection does exist (with 0 row) => dropping it (requested with --drop CLI argument)");
									sodaCollection.admin().drop();
									REPORT.getCollection(collectionName).wasDropped = true;
									LOGGER.info((mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection does exist => re-creating it");
									sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection(db, ret.collectionName, forceOSON, collectionsProperties) : createClassicCollection(db, ret.collectionName, forceOSON);
									if (sodaCollection == null) {
										throw new IllegalStateException("Can't re-create " + (mongoDBAPICompatible ? "MongoDB API compatible" : "SODA") + " collection: " + ret.collectionName);
									}
								}
							}
						}
/*
                        for (Document indexMetadata : mongoCollection.listIndexes()) {
                            if (indexMetadata.getString("name").contains("$**") || "text".equals(indexMetadata.getEmbedded(Arrays.asList("key"), Document.class).getString("_fts"))) {
                                ret.needSearchIndex = true;
                            }
                        }

/*                        if (ret.needSearchIndex) {
                            long start = System.currentTimeMillis();
                            s.execute(String.format("CREATE SEARCH INDEX %s$search_index ON %s (json_document) FOR JSON PARAMETERS('DATAGUIDE OFF SYNC(MANUAL)')", collectionName, collectionName));
                            System.out.println("Created Search Index (manual sync) in " + getDurationSince(start));
                        } */
					}
				}
			}

			try (PreparedStatement p = c.prepareStatement("select constraint_name, search_condition, status from all_constraints where owner=? and table_name=? and constraint_type='C'")) {
				p.setString(1, user);
				p.setString(2, ret.tableName);

				try (ResultSet r = p.executeQuery()) {
					while (r.next()) {
						final String constraintText = r.getString(2);
						final String condition = constraintText.toLowerCase();
						if (condition.contains("is json") && condition.contains(mongoDBAPICompatible ? "data" : "json_document")) {
							ret.isJsonConstraintName = r.getString(1);
							ret.isJsonConstraintEnabled = r.getString(3).equalsIgnoreCase("ENABLED");
							ret.isJsonConstraintText = constraintText;
							ret.isJsonConstraintIsOSONFormat = condition.contains("format oson");
							LOGGER.info("IS JSON constraint found: " + ret.isJsonConstraintName + " for collection " + ret.collectionName);
							break;
						}
					}
				}

				if (ret.isJsonConstraintName != null) {
					LOGGER.info("Disabling Is JSON constraint: " + ret.isJsonConstraintName);
					p.execute("alter table " + user + ".\"" + ret.tableName + "\" disable constraint " + ret.isJsonConstraintName);
				}
			}

			try (PreparedStatement p = c.prepareStatement("select index_name, status from all_indexes where table_owner=? and table_name=? and uniqueness='UNIQUE' and constraint_index='YES'")) {
				p.setString(1, user);
				p.setString(2, ret.tableName);

				try (ResultSet r = p.executeQuery()) {
					if (r.next()) {
						ret.primaryKeyIndexName = r.getString(1);
						ret.primaryKeyIndexStatus = r.getString(2);
						LOGGER.info("Primary key found: " + ret.primaryKeyIndexName + " for collection " + ret.collectionName + " with status " + ret.primaryKeyIndexStatus);
					}
				}

				if (ret.primaryKeyIndexName != null /*&& !autonomousDatabase*/) {
					LOGGER.info("Dropping primary key and associated index: " + ret.primaryKeyIndexName + " for collection " + ret.collectionName);
					LOGGER.info("Running: " + "alter table " + user + ".\"" + ret.tableName + "\" drop primary key drop index");
					p.execute("alter table " + user + ".\"" + ret.tableName + "\" drop primary key drop index");
					LOGGER.info("Running: " + "alter table " + user + ".\"" + ret.tableName + "\" drop primary key drop index DONE!");
				}
			}
		}

		return ret;
	}

	private static OracleCollection createClassicCollection(OracleDatabase db, String collectionName, boolean forceOSON) throws OracleException, SQLException {
		final int version = db.admin().getConnection().getMetaData().getDatabaseMajorVersion();

		LOGGER.info("Connected to database v" + version);

		if (forceOSON) {
			try (CallableStatement cs = db.admin().getConnection().prepareCall("{call DBMS_SODA_ADMIN.CREATE_COLLECTION(P_URI_NAME => ?, P_CREATE_MODE => 'NEW', P_DESCRIPTOR => ?, P_CREATE_TIME => ?) }")) {
				final String metadata = version == 19 ?
						"""
								{
									"contentColumn" : {
									   "name" : "JSON_DOCUMENT",
									   "sqlType" : "BLOB",
									   "jsonFormat" : "OSON"
									},
									"keyColumn" : {
									   "name" : "ID",
										"method" : "UUID"
									},
									"versionColumn" : {
										"name" : "VERSION",
										"method" : "UUID"
									},
									"lastModifiedColumn" : {
										"name" : "LAST_MODIFIED"
									},
									"creationTimeColumn" : {
										"name" : "CREATED_ON"
									}
								}"""
						: """
						    				{
						    "contentColumn" : {
						       "name" : "JSON_DOCUMENT"
						    },
						    "keyColumn" : {
						       "name" : "ID",
						       "method" : "UUID"
						    },
						    "versionColumn" : {
						        "name" : "VERSION",
						        "method" : "UUID"
						    },
						    "lastModifiedColumn" : {
						        "name" : "LAST_MODIFIED"
						    },
						    "creationTimeColumn" : {
						        "name" : "CREATED_ON"
						    }
						}""";

				LOGGER.info("Using metadata: " + metadata);

				cs.registerOutParameter(3, Types.VARCHAR);
				cs.setString(1, collectionName);
				cs.setString(2, metadata);

				cs.execute();
			}

			return db.openCollection(collectionName);
		}
		else {
			LOGGER.info("Using default metadata");

			return db.admin().createCollection(collectionName);
		}
	}

	/**
	 *
	 */
	private static OracleCollection createMongoDBAPICompatibleCollection(OracleDatabase db, String collectionName, boolean forceOSON, Properties collectionsProperties) throws SQLException, OracleException {
		final int version = db.admin().getConnection().getMetaData().getDatabaseMajorVersion();

		String IDproperty = collectionsProperties.getProperty(collectionName + ".ID", "EMBEDDED_OID");

		LOGGER.info("Found overriding property for ID column: " + IDproperty);

		if (version == 19 || version == 21 || (forceOSON && version < 23)) {
			try (CallableStatement cs = db.admin().getConnection().prepareCall("{call DBMS_SODA_ADMIN.CREATE_COLLECTION(P_URI_NAME => ?, P_CREATE_MODE => 'NEW', P_DESCRIPTOR => ?, P_CREATE_TIME => ?) }")) {
				final String metadata = version == 19 || forceOSON ?
						"""
								{
									"contentColumn" : {
									   "name" : "DATA",
									   "sqlType" : "BLOB",
									   "jsonFormat" : "OSON"
									},
									"keyColumn" : {
									   "name" : "ID",
									   "assignmentMethod" : "EMBEDDED_OID",
									   "path" : "_id"
									},
									"versionColumn" : {
										"name" : "VERSION",
										"method" : "UUID"
									},
									"lastModifiedColumn" : {
										"name" : "LAST_MODIFIED"
									},
									"creationTimeColumn" : {
										"name" : "CREATED_ON"
									}
								}"""
						: """
						    				{
						    "contentColumn" : {
						       "name" : "DATA"
						    },
						    "keyColumn" : {
						       "name" : "ID",
						       "assignmentMethod" : "EMBEDDED_OID",
						       "path" : "_id"
						    },
						    "versionColumn" : {
						        "name" : "VERSION",
						        "method" : "UUID"
						    },
						    "lastModifiedColumn" : {
						        "name" : "LAST_MODIFIED"
						    },
						    "creationTimeColumn" : {
						        "name" : "CREATED_ON"
						    }
						}""";

				LOGGER.info("Using metadata: " + metadata);

				cs.registerOutParameter(3, Types.VARCHAR);
				cs.setString(1, collectionName);
				cs.setString(2, metadata);

				cs.execute();
			}

			return db.openCollection(collectionName);
		}
		else { // version >= 23
			try (CallableStatement cs = db.admin().getConnection().prepareCall("{call DBMS_SODA_ADMIN.CREATE_COLLECTION(P_URI_NAME => ?, P_CREATE_MODE => 'NEW', P_DESCRIPTOR => ?, P_CREATE_TIME => ?, P_23C_DRIVER => true) }")) {
				final String metadata =
						String.format("""
								{
								    "contentColumn" : {
								       "name" : "DATA",
								       "sqlType" : "JSON"
								    },
								    "keyColumn" : {
								       "name" : "ID",
								       "sqlType": "RAW",
								       %s
								    },
								    "versionColumn" : {
								        "name" : "VERSION",
								        "method" : "UUID"
								    },
								    "lastModifiedColumn" : {
								        "name" : "LAST_MODIFIED"
								    },
								    "creationTimeColumn" : {
								        "name" : "CREATED_ON"
								    },
								    "readOnly" : false
								}""", "EMBEDDED_OID".equalsIgnoreCase(IDproperty) ? "\"assignmentMethod\" : \"EMBEDDED_OID\", \"path\" : \"_id\"" :
								String.format("\"assignmentMethod\" : \"%s\"", IDproperty));

				LOGGER.info("Using metadata: " + metadata);

				cs.registerOutParameter(3, Types.VARCHAR);
				cs.setString(1, collectionName);
				cs.setString(2, metadata);

				cs.execute();
			}

			return db.openCollection(collectionName);
		}
	}

	// https://github.com/oracle/json-in-db/tree/master/MigrationTools/IndexParser

	/**
	 * Using medium service reconfigured with standard user.
	 *
	 * @param mediumPDS
	 * @param mongoCollection
	 * @throws SQLException
	 */
	public void finish(PoolDataSource mediumPDS, MongoCollection<Document> mongoCollection, MongoDBMetadata collectionMetadataDump, Configuration conf, ASCIIGUI gui, int oracleVersion) throws SQLException, OracleException {
		final int maxParallelDegree = conf.maxSQLParallelDegree;
		final boolean mongoDBAPICompatible = conf.mongodbAPICompatible;
		final boolean skipSecondaryIndexes = conf.skipSecondaryIndexes;
		final boolean buildSecondaryIndexes = conf.buildSecondaryIndexes;

		try (Connection c = mediumPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				if (!buildSecondaryIndexes) {
					LOGGER.info("Enabling JSON constraint...");
					if (isJsonConstraintName != null) {
						LOGGER.info("Re-Enabling Is JSON constraint NOVALIDATE");
						s.execute("alter table \"" + tableName + "\" modify constraint " + isJsonConstraintName + " enable novalidate");
					}
					else {
						if (c.getMetaData().getDatabaseMajorVersion() == 19) {
							LOGGER.info("Creating Is JSON constraint NOVALIDATE");
							s.execute("alter table \"" + tableName + "\" add constraint " + collectionName + "_jd_is_json check (" + (mongoDBAPICompatible ? "data" : "json_document") + " is json format oson (size limit 32m)) novalidate");
						}
					}
					LOGGER.info("OK");
				}

				// MOS 473656.1
				s.execute("ALTER SESSION ENABLE PARALLEL DDL");

				if (!buildSecondaryIndexes) {
					gui.startIndex("primary key");

					LOGGER.info("Adding primary key constraint and index...");
					long start = System.currentTimeMillis();
					if (primaryKeyIndexName != null) {
						String currentPKIndexStatus = "?";
						try (PreparedStatement p = c.prepareStatement("select status from all_indexes where table_owner=? and table_name=? and index_name=? and uniqueness='UNIQUE' and constraint_index='YES'")) {
							p.setString(1, user);
							p.setString(2, tableName);
							p.setString(3, primaryKeyIndexName);

							try (ResultSet r = p.executeQuery()) {
								if (r.next()) {
									currentPKIndexStatus = r.getString(1);
								}
							}
						}

						if (false && autonomousDatabase && "UNUSABLE".equalsIgnoreCase(currentPKIndexStatus)) {


							LOGGER.info("ALTER INDEX " + primaryKeyIndexName + " REBUILD PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							s.execute("ALTER INDEX " + primaryKeyIndexName + " REBUILD PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							LOGGER.info("Rebuild PK index with parallel degree of " + (2 * maxParallelDegree) + " in " + getDurationSince(start));
						}
						else {
							LOGGER.info("PK Index status is: " + currentPKIndexStatus);
							if ("?".equals(currentPKIndexStatus)) {
								LOGGER.info("CREATE UNIQUE INDEX " + user + "." + primaryKeyIndexName + " ON " + user + ".\"" + tableName + "\" (ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
								s.execute("CREATE UNIQUE INDEX " + user + "." + primaryKeyIndexName + " ON " + user + ".\"" + tableName + "\" (ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
								LOGGER.info("ALTER TABLE " + user + ".\"" + tableName + "\" ADD CONSTRAINT " + primaryKeyIndexName + " PRIMARY KEY (ID) USING INDEX " + user + "." + primaryKeyIndexName + " ENABLE NOVALIDATE");
								s.execute("ALTER TABLE " + user + ".\"" + tableName + "\" ADD CONSTRAINT " + primaryKeyIndexName + " PRIMARY KEY (ID) USING INDEX " + user + "." + primaryKeyIndexName + " ENABLE NOVALIDATE");
								LOGGER.info("Created PK constraint and index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
							}
						}
					}
					else {
						try {
							LOGGER.info("CREATE UNIQUE INDEX " + "PK_" + collectionName + " ON \"" + tableName + "\" (ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							s.execute("CREATE UNIQUE INDEX " + "PK_" + collectionName + " ON \"" + tableName + "\" (ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							LOGGER.info("ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT PK_" + collectionName + " PRIMARY KEY (ID) USING INDEX " + "PK_" + collectionName + " ENABLE NOVALIDATE");
							s.execute("ALTER TABLE \"" + tableName + "\" ADD CONSTRAINT PK_" + collectionName + " PRIMARY KEY (ID) USING INDEX " + "PK_" + collectionName + " ENABLE NOVALIDATE");
							LOGGER.info("Created PK constraint and index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
						}
						catch (SQLException sqle) {
							sqle.printStackTrace();
						}
					}

					gui.endIndex("primary key", true);
					REPORT.getCollection(collectionName).addIndex(primaryKeyIndexName != null ? primaryKeyIndexName : "PK_"+collectionName, IndexType.PRIMARY_KEY);
				}

				// manage other MongoDB indexes
				int mongoDBIndex = 0;

				if (mongoCollection != null && !skipSecondaryIndexes) {
					for (Document indexMetadata : mongoCollection.listIndexes()) {
						mongoDBIndex++;
					}

					if (mongoDBIndex > 0) {
						final Properties props = new Properties();
						props.put("oracle.soda.sharedMetadataCache", "true");
						props.put("oracle.soda.localMetadataCache", "true");

						final OracleRDBMSClient cl = new OracleRDBMSClient(props);
						final OracleDatabase db = cl.getDatabase(c);
						final OracleCollection sodaCollection = db.openCollection(collectionName);

						final Map<String, FieldInfo> fieldsInfo = new TreeMap<>();

						try (ResultSet r = s.executeQuery("with dg as (select json_object( 'dg' : json_dataguide( " + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ", dbms_json.format_flat, DBMS_JSON.GEOJSON+DBMS_JSON.GATHER_STATS) format JSON returning clob) as json_document from \"" + tableName + "\" where rownum <= 1000)\n" +
								"select u.field_path, type, length, low, high from dg nested json_document columns ( nested dg[*] columns (field_path path '$.\"o:path\"', type path '$.type', length path '$.\"o:length\"', low path '$.\"o:low_value\"', high path '$.\"o:high_value\"' )) u")) {
							while (r.next()) {
								String key = r.getString(1);
								if (!r.wasNull()) {
									fieldsInfo.put(key, new FieldInfo(key, r.getString(2), r.getInt(3), r.getString(4), r.getString(5)));
								}
							}
						}

						boolean needSearchIndex = false;

						for (Document indexMetadata : mongoCollection.listIndexes()) {
							LOGGER.info(indexMetadata.toString());
							if (indexMetadata.getLong("expireAfterSeconds") != null) {
								LOGGER.warn("TTL index " + indexMetadata.getString("name") + " with data expiration after " + indexMetadata.getLong("expireAfterSeconds") + " seconds not recreated!");
							}
							else if (indexMetadata.getString("name").contains("$**") || "text".equals(indexMetadata.getEmbedded(Arrays.asList("key"), Document.class).getString("_fts"))) {
								needSearchIndex = true; //System.out.println("Need Search Index");
							}
							/*else if (indexMetadata.getString("name").equals("_id_")) {
								// skipping primary key as it already exists now!
								continue;
							}*/
							else {
								final Document keys = indexMetadata.getEmbedded(Arrays.asList("key"), Document.class);
								boolean spatial = false;
								String spatialColumn = "";
								for (String key : keys.keySet()) {
									final Object value = keys.get(key);
									if (value instanceof Integer) continue;
									if (value instanceof String) {
										if ("2dsphere".equals(value)) {
											spatialColumn = key;
											spatial = true;
											break;
										}
									}
								}

								if (spatial) {
									LOGGER.info("Spatial index");
									final MongoCursor<Document> cursor = mongoCollection.find(new Document(spatialColumn + ".type", "Point")).projection(new Document(spatialColumn + ".type", 1)).batchSize(100).limit(100).cursor();

									boolean allDocsHavePoints = false;

									int numberOfDocsWithPoint = 0;
									while (cursor.hasNext()) {
										cursor.next();
										numberOfDocsWithPoint++;
									}

									allDocsHavePoints = numberOfDocsWithPoint == 100;

									final String SQLStatement = String.format(
											"create index %s on \"%s\" " +
													"(JSON_VALUE(" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ", '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
													"indextype is MDSYS.SPATIAL_INDEX_V2" + (allDocsHavePoints ? " PARAMETERS ('layer_gtype=POINT cbtree_index=true')" : "") + (maxParallelDegree == -1 ? "" : " parallel " + maxParallelDegree), collectionName + "$" + indexMetadata.getString("name"), tableName, spatialColumn);

									LOGGER.info(SQLStatement);
									long start = System.currentTimeMillis();
									gui.startIndex(indexMetadata.getString("name"));
									s.execute(SQLStatement);
									LOGGER.info("Created spatial index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
									gui.endIndex(indexMetadata.getString("name"), true);
									REPORT.getCollection(collectionName).addIndex(indexMetadata.getString("name"), IndexType.GEO_JSON);
								}
								else if (is_IdPK(keys)) {
									/*LOGGER.info("_id field index");
									final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [{\"path\": \"_id\", \"order\": \"%s\"}], \"unique\": true}", collectionName + "$" + indexMetadata.getString("name"), keys.getInteger("_id") == 1 ? "asc" : "desc");

									LOGGER.info(indexSpec);
									long start = System.currentTimeMillis();*/
									gui.startIndex(indexMetadata.getString("name"));
									/*sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
									LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));*/
									gui.endIndex(indexMetadata.getString("name"), true);

									for(IndexReport ir : REPORT.getCollection(collectionName).indexes) {
										if(ir.type == IndexType.PRIMARY_KEY) {
											s.execute("alter index "+(primaryKeyIndexName != null ? primaryKeyIndexName : "PK_"+collectionName)+" rename to \""+ collectionName + "$"+indexMetadata.getString("name")+"\"");
											REPORT.getCollection(collectionName).replaceIndex(ir.name,collectionName + "$"+indexMetadata.getString("name"));
											break;
										}
									}
								}
								else {
									LOGGER.info("Normal index");
									final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + indexMetadata.getString("name"), getCreateIndexColumnsForMongoDBDatabase(collectionName, keys, fieldsInfo, oracleVersion),
											indexMetadata.getBoolean("unique") == null ? "false" : indexMetadata.getBoolean("unique"));

									LOGGER.info(indexSpec);
									long start = System.currentTimeMillis();
									gui.startIndex(indexMetadata.getString("name"));
									sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
									LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
									gui.endIndex(indexMetadata.getString("name"), true);
									REPORT.getCollection(collectionName).addIndex(collectionName + "$"+indexMetadata.getString("name"), keys.keySet().size() == 1 ? IndexType.SIMPLE : IndexType.COMPOUND);
									REPORT.getCollection(collectionName).getIndex(collectionName + "$"+indexMetadata.getString("name")).numberOfFields = keys.keySet().size();
								}
							}
						}

						if (needSearchIndex) {
                    /*try (CallableStatement cs = c.prepareCall("{CALL CTX_DDL.SYNC_INDEX(idx_name => ?, memory => '512M', parallel_degree => ?, locking => CTX_DDL.LOCK_NOWAIT)}")) {
                        cs.setString(1, collectionName + "$search_index");
                        cs.setInt(2, maxParallelDegree);
                        start = System.currentTimeMillis();
                        cs.execute();
                        System.out.println("Manual sync of Search Index done in " + getDurationSince(start));
                    }*/

							long start = System.currentTimeMillis();
							gui.startIndex("search_index");
							s.execute(String.format("CREATE SEARCH INDEX %s$search_index ON \"%s\" (" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ") FOR JSON PARAMETERS('DATAGUIDE OFF SYNC(every \"freq=secondly;interval=1\" MEMORY 2G parallel %d)')", collectionName, tableName, maxParallelDegree));
							LOGGER.info("Created Search Index (every 1s sync) in " + getDurationSince(start));
							gui.endIndex("search_index", true);
							REPORT.getCollection(collectionName).addIndex(String.format("%s$search_index",collectionName), IndexType.JSON_SEARCH);
						}

						s.execute("ALTER SESSION DISABLE PARALLEL DDL");
						LOGGER.info("OK");
					}
				}
				// MANAGE DUMP METADATA NOW
				else if (collectionMetadataDump != null && !skipSecondaryIndexes) {
					for (MetadataIndex indexMetadata : collectionMetadataDump.getIndexes()) {
						mongoDBIndex++;
					}

					if (mongoDBIndex > 0 && !conf.buildIndexesLikeMiguel) {
						final Properties props = new Properties();
						props.put("oracle.soda.sharedMetadataCache", "true");
						props.put("oracle.soda.localMetadataCache", "true");

						final OracleRDBMSClient cl = new OracleRDBMSClient(props);
						final OracleDatabase db = cl.getDatabase(c);
						final OracleCollection sodaCollection = db.openCollection(collectionName);

						final Map<String, FieldInfo> fieldsInfo = new TreeMap<>();

						try (ResultSet r = s.executeQuery("with dg as (select json_object( 'dg' : json_dataguide( " + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ", dbms_json.format_flat, DBMS_JSON.GEOJSON+DBMS_JSON.GATHER_STATS) format JSON returning clob) as json_document from \"" + tableName + "\" where rownum <= 1000)\n" +
								"select u.field_path, type, length, low, high from dg nested json_document columns ( nested dg[*] columns (field_path path '$.\"o:path\"', type path '$.type', length path '$.\"o:length\"', low path '$.\"o:low_value\"', high path '$.\"o:high_value\"' )) u")) {
							while (r.next()) {
								String key = r.getString(1);
								if (!r.wasNull()) {
									fieldsInfo.put(key, new FieldInfo(key, r.getString(2), r.getInt(3), r.getString(4), r.getString(5)));
								}
							}
						}

						boolean needSearchIndex = false;

						for (MetadataIndex indexMetadata : collectionMetadataDump.getIndexes()) {
							LOGGER.info(indexMetadata.toString());
							if (indexMetadata.isTtl()) {
								LOGGER.warn("TTL index " + indexMetadata.getName() + " with data expiration after " + indexMetadata.getExpireAfterSeconds().value + " seconds not recreated!");
							}
							else if (indexMetadata.getName().contains("$**") || indexMetadata.getKey().text) {
								needSearchIndex = true;
							}
							/*else if (indexMetadata.getName().equals("_id_")) {
								// skipping primary key as it already exists now!
								continue;
							}*/
							else {

								boolean spatial = false;
								String spatialColumn = "";
								if (indexMetadata.getKey().spatial) {
									spatialColumn = indexMetadata.getKey().columns.get(0).name;
									spatial = true;
								}

								if (spatial) {
									LOGGER.info("Spatial index");
									final MongoCursor<Document> cursor = mongoCollection.find(new Document(spatialColumn + ".type", "Point")).projection(new Document(spatialColumn + ".type", 1)).batchSize(100).limit(100).cursor();

									boolean allDocsHavePoints = false;

									int numberOfDocsWithPoint = 0;
									while (cursor.hasNext()) {
										cursor.next();
										numberOfDocsWithPoint++;
									}

									allDocsHavePoints = numberOfDocsWithPoint == 100;

									final String SQLStatement = String.format(
											"create index %s on \"%s\" " +
													"(JSON_VALUE(" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ", '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
													"indextype is MDSYS.SPATIAL_INDEX_V2" + (allDocsHavePoints ? " PARAMETERS ('layer_gtype=POINT cbtree_index=true')" : "") + (maxParallelDegree == -1 ? "" : " parallel " + maxParallelDegree), collectionName + "$" + indexMetadata.getName(), tableName, spatialColumn);

									LOGGER.info(SQLStatement);
									long start = System.currentTimeMillis();
									gui.startIndex(indexMetadata.getName());
									s.execute(SQLStatement);
									LOGGER.info("Created spatial index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
									gui.endIndex(indexMetadata.getName(), true);
									REPORT.getCollection(collectionName).addIndex(collectionName + "$"+indexMetadata.getName(), IndexType.GEO_JSON);
								}
								else if (is_IdPK(indexMetadata.getKey())) {
									/*LOGGER.info("_id field index");
									final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [{\"path\": \"_id\", \"order\": \"%s\"}], \"unique\": true}", collectionName + "$" + indexMetadata.getName(), getId_PKOrder( indexMetadata.getKey() ));

									LOGGER.info(indexSpec);
									long start = System.currentTimeMillis();*/
									gui.startIndex(indexMetadata.getName());
									/*sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
									LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));*/
									gui.endIndex(indexMetadata.getName(), true);

									for(IndexReport ir : REPORT.getCollection(collectionName).indexes) {
										if(ir.type == IndexType.PRIMARY_KEY) {
											s.execute("alter index "+(primaryKeyIndexName != null ? primaryKeyIndexName : "PK_"+collectionName)+" rename to \""+ collectionName + "$"+indexMetadata.getName()+"\"");
											REPORT.getCollection(collectionName).replaceIndex(ir.name,collectionName + "$"+indexMetadata.getName());
											break;
										}
									}
								}
								else {
									LOGGER.info("Normal index");

									if (indexMetadata.getKey().hasColumns()) {
										String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + indexMetadata.getName(), getCreateIndexColumnsForMongoDBDump(collectionName, indexMetadata.getKey(), fieldsInfo, oracleVersion, false), indexMetadata.isUnique());

										LOGGER.info(indexSpec);
										long start = System.currentTimeMillis();
										gui.startIndex(indexMetadata.getName());
										try {
											sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));

											LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
											gui.endIndex(indexMetadata.getName(), true);
											REPORT.getCollection(collectionName).addIndex(collectionName + "$"+indexMetadata.getName(), indexMetadata.getKey().isCompound() ? IndexType.COMPOUND : IndexType.SIMPLE);
											REPORT.getCollection(collectionName).getIndex(collectionName + "$"+indexMetadata.getName()).numberOfFields = indexMetadata.getKey().getNumberOfFields();
										}
										catch (OracleException oe) {
											LOGGER.error("Creating index", oe);
											if (oe.getCause() instanceof SQLException) {
												SQLException sqle = (SQLException) oe.getCause();
												LOGGER.error("Error " + sqle.getErrorCode() + ", " + sqle.getMessage().contains("evaluated to multiple values"));

												/*if(sqle.getMessage().contains("evaluated to multiple values")) {
													indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s, \"multivalue\": true}", collectionName + "$" + indexMetadata.getName(), getCreateIndexColumnsForMongoDBDump(collectionName, indexMetadata.getKey(), fieldsInfo, oracleVersion, true), indexMetadata.isUnique() );

													LOGGER.info(indexSpec);

													sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));

													LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
												}*/
											}
											gui.endIndex(indexMetadata.getName(), false);
											REPORT.getCollection(collectionName).addFailedIndex(indexMetadata.getName(),indexMetadata.getKey().isCompound() ? IndexType.COMPOUND : IndexType.SIMPLE);
											REPORT.getCollection(collectionName).getFailedIndex(indexMetadata.getName()).numberOfFields = indexMetadata.getKey().getNumberOfFields();
										}
									}
									else {
										final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + indexMetadata.getName(), getCreateIndexColumnsForMongoDBDump(collectionName, indexMetadata.getKey(), fieldsInfo, oracleVersion, false), indexMetadata.isUnique());

										LOGGER.warn("Index has no column!");
										LOGGER.warn(indexSpec);
										long start = System.currentTimeMillis();
										gui.startIndex(indexMetadata.getName());
										LOGGER.warn("Standard SODA index not created!");
										gui.endIndex(indexMetadata.getName(), false);
									}
								}
							}
						}

						if (needSearchIndex) {
                    /*try (CallableStatement cs = c.prepareCall("{CALL CTX_DDL.SYNC_INDEX(idx_name => ?, memory => '512M', parallel_degree => ?, locking => CTX_DDL.LOCK_NOWAIT)}")) {
                        cs.setString(1, collectionName + "$search_index");
                        cs.setInt(2, maxParallelDegree);
                        start = System.currentTimeMillis();
                        cs.execute();
                        System.out.println("Manual sync of Search Index done in " + getDurationSince(start));
                    }*/

							long start = System.currentTimeMillis();
							gui.startIndex("search_index");
							s.execute(String.format("CREATE SEARCH INDEX %s$search_index ON \"%s\" (" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ") FOR JSON PARAMETERS('DATAGUIDE OFF SYNC(every \"freq=secondly;interval=1\" MEMORY 2G parallel %d)')", collectionName, tableName, maxParallelDegree));
							LOGGER.info("Created Search Index (every 1s sync) in " + getDurationSince(start));
							gui.endIndex("search_index", true);
							REPORT.getCollection(collectionName).addIndex(String.format("%s$search_index",collectionName), IndexType.JSON_SEARCH);
						}

						s.execute("ALTER SESSION DISABLE PARALLEL DDL");
						LOGGER.info("OK");
					}
					else if (mongoDBIndex > 0 && conf.buildIndexesLikeMiguel) {
						MiguelIndexes.build(this,c,conf,collectionMetadataDump.getIndexes(), oracleVersion, gui);
					}
				}
			}
		}
		catch (SQLException sqle) {
			LOGGER.error("During finish step of collection " + collectionName, sqle);
			throw sqle;
		}
	}

	private String getId_PKOrder(MetadataKey keys) {
		for (IndexColumn column : keys.columns) {
			return column.asc ? "asc" : "desc";
		}

		return "asc";
	}

	private boolean is_IdPK(MetadataKey keys) {
		final StringBuilder s = new StringBuilder();
		for (IndexColumn column : keys.columns) {
			if (!s.isEmpty()) {
				s.append(", ");
			}
			s.append(column.name);
		}

		return "_id".contentEquals(s);
	}

	private boolean is_IdPK(Document keys) {
		final StringBuilder s = new StringBuilder();
		for (String columnName : keys.keySet()) {
			if (!s.isEmpty()) {
				s.append(", ");
			}
			s.append(columnName);
		}

		return "_id".contentEquals(s);
	}

	private String getCreateIndexColumnsForMongoDBDatabase(String collectionName, Document keys, Map<String, FieldInfo> fieldsInfo, int oracleVersion) {
		final StringBuilder s = new StringBuilder();

		for (String columnName : keys.keySet()) {
			if (!s.isEmpty()) {
				s.append(", ");
			}

           /* if (cantIndex.contains(ic.name)) {
                if (s.length() > 0) {
                    s.setLength(s.length() - 2);
                }

                log.warn("For collection " + collectionName + ", the field " + ic.name + " has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which path belongs to an array");
                warning = true;
            }
            else if (oracleMetadata.containsKey(collectionName + "." + ic.name + ".datatype")) {
                final String dataType = (String) oracleMetadata.get(collectionName + "." + ic.name + ".datatype");

                switch (dataType) {
                    case "number":
                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        break;

                    case "string":
                        final String dataLength = (String) oracleMetadata.get(collectionName + "." + ic.name + ".maxlength");

                        if (Integer.parseInt(dataLength) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                            log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + dataLength + " bytes, defined in oracle metadata file) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                            warning = true;
                        }

                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(dataLength).append("}");
                        break;

                    default:
                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                        break;
                }
            }
            else {
                if (fieldsDataTypes.containsKey(ic.name)) {
                    if (fieldsDataTypes.get(ic.name).size() == 1) {
                        final String dataType = fieldsDataTypes.get(ic.name).iterator().next();
                        if ("number".equals(dataType)) {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        }
                        else if ("array".equals(dataType)) {
                            // remove the trailing ", "
                            if (s.length() > 0) {
                                s.setLength(s.length() - 2);
                            }

                            log.warn("For collection " + collectionName + ", the field " + ic.name + " has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which is an array");
                            warning = true;
                            //s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        }
                        else {
                            if (maxLengths.containsKey(ic.name)) {
                                if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                    log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                                    warning = true;
                                }
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                            }
                            else {
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                            }
                        }
                    }
                    else {
                        if (maxLengths.containsKey(ic.name)) {
                            if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                                warning = true;
                            }
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                        }
                        else {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\"}");
                        }
                    }
                }
                else if (maxLengths.containsKey(ic.name)) {
                    if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                        log.warn("For collection " + collectionName + ", index " + name + " has a field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                        warning = true;
                    }
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                }
                else {
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                }
            }*/

			final FieldInfo fi = fieldsInfo.get("$." + columnName);

			if (oracleVersion >= 23) {
				s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\", \"datatype\": \"json\"}");
			}
			else {
				if (fi == null) {
					if (oracleVersion >= 23) {
						s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\", \"datatype\": \"json\"}");
					}
					else {
						s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\"}");
					}
				}
				else {
					s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\", \"datatype\": \"").append(fi.type).append("\"");
					if ("string".equals(fi.type)) {
						s.append(", \"maxlength\": ").append(fi.length);
					}
					s.append("}");
				}
			}
		}

		return s.toString();
	}

	private static String getCreateIndexColumnsForMongoDBDump(String collectionName, MetadataKey keys, Map<String, FieldInfo> fieldsInfo, int oracleVersion, boolean multivalue) {
		final StringBuilder s = new StringBuilder();

		for (IndexColumn column : keys.columns) {
			String columnName = column.name;

			if (!s.isEmpty()) {
				s.append(", ");
			}

           /* if (cantIndex.contains(ic.name)) {
                if (s.length() > 0) {
                    s.setLength(s.length() - 2);
                }

                log.warn("For collection " + collectionName + ", the field " + ic.name + " has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which path belongs to an array");
                warning = true;
            }
            else if (oracleMetadata.containsKey(collectionName + "." + ic.name + ".datatype")) {
                final String dataType = (String) oracleMetadata.get(collectionName + "." + ic.name + ".datatype");

                switch (dataType) {
                    case "number":
                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        break;

                    case "string":
                        final String dataLength = (String) oracleMetadata.get(collectionName + "." + ic.name + ".maxlength");

                        if (Integer.parseInt(dataLength) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                            log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + dataLength + " bytes, defined in oracle metadata file) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                            warning = true;
                        }

                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(dataLength).append("}");
                        break;

                    default:
                        s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                        break;
                }
            }
            else {
                if (fieldsDataTypes.containsKey(ic.name)) {
                    if (fieldsDataTypes.get(ic.name).size() == 1) {
                        final String dataType = fieldsDataTypes.get(ic.name).iterator().next();
                        if ("number".equals(dataType)) {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        }
                        else if ("array".equals(dataType)) {
                            // remove the trailing ", "
                            if (s.length() > 0) {
                                s.setLength(s.length() - 2);
                            }

                            log.warn("For collection " + collectionName + ", the field " + ic.name + " has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which is an array");
                            warning = true;
                            //s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        }
                        else {
                            if (maxLengths.containsKey(ic.name)) {
                                if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                    log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                                    warning = true;
                                }
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                            }
                            else {
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                            }
                        }
                    }
                    else {
                        if (maxLengths.containsKey(ic.name)) {
                            if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                log.warn("For collection " + collectionName + ", index " + name + " has a string field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                                warning = true;
                            }
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                        }
                        else {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\"}");
                        }
                    }
                }
                else if (maxLengths.containsKey(ic.name)) {
                    if (maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                        log.warn("For collection " + collectionName + ", index " + name + " has a field \"" + ic.name + "\" with a maxlength (" + maxLengths.get(ic.name) + " bytes) strictly larger than the threshold " + INDEXED_FIELD_MAX_LENGTH_WARNING);
                        warning = true;
                    }
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                }
                else {
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                }
            }*/

			final FieldInfo fi = fieldsInfo.get("$." + columnName);

			if (oracleVersion >= 23) {
				if (multivalue) {
					s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(column.asc ? "asc" : "desc").append("\"}");
				}
				else {
					s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(column.asc ? "asc" : "desc").append("\", \"datatype\": \"json\"}");
				}
			}
			else {
				if (fi == null) {
					s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(column.asc ? "asc" : "desc").append("\"}");
				}
				else {
					s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(column.asc ? "asc" : "desc").append("\", \"datatype\": \"").append(fi.type).append("\"");
					if ("string".equals(fi.type)) {
						s.append(", \"maxlength\": ").append(fi.length);
					}
					s.append("}");
				}
			}
		}

		return s.toString();
	}

	public String getTableName() {
		return tableName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	static class FieldInfo {
		public String low;
		public String high;
		public String path;
		public String type;
		public int length;

		public FieldInfo() {
		}

		public FieldInfo(String path, String type, int length, String lowValue, String highValue) {
			this.path = path;
			this.type = type;
			this.length = length;
			this.low = lowValue;
			this.high = lowValue;
		}

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		}

		public String getLow() {
			return low;
		}

		public void setLow(String low) {
			this.low = low;
		}

		public String getHigh() {
			return high;
		}

		public void setHigh(String high) {
			this.high = high;
		}
	}
/*
	public static void main(String[] args) throws Throwable {
		Locale.setDefault(Locale.US);
		try(Connection c = DriverManager.getConnection("jdbc:oracle:thin:@oracledb23c/freepdb1","developer","free")) {
			final Properties props = new Properties();
			props.put("oracle.soda.sharedMetadataCache", "true");
			props.put("oracle.soda.localMetadataCache", "true");

			final OracleRDBMSClient cl = new OracleRDBMSClient(props);
			final OracleDatabase db = cl.getDatabase(c);
			OracleCollection sodaCollection = db.openCollection("GRUPPO_BPER_TERZA");

			String indexSpec = """
					{"name": "GRUPPO_BPER_TERZA$datapoint_asset_test", "fields": [{"path": "_id.GRUPPO", "order": "asc", "dataType": "json"}, 
					                                                         {"path": "_id.ABI", "order": "asc", "dataType": "json"}, 
					                                                         {"path": "_id.DATA", "order": "asc", "dataType": "json"},
					                                                         {"path": "LAYER_INDIVIDUALE.LAYER_OUTPUT.TEMPLATE_ASSET_ENCUMBRANCE.DATI.DATA_POINT", "order": "asc", "dataType": "json"}], "unique": false}""";

			String indexDDL = "create  multivalue index \"GRUPPO_BPER_TERZA$datapoint_asset\" on \"GRUPPO_BPER_TERZA\" d (\n" +
					"d.DATA.LAYER_INDIVIDUALE.LAYER_OUTPUT.TEMPLATE_ASSET_ENCUMBRANCE.DATI.DATA_POINT.string())"; // <-- needs first to check the type of scalars inside the array!

//			String indexSpec = """
//					{"name": "GRUPPO_BPER_TERZA$datapoint_asset", "fields": [{"path": "LAYER_INDIVIDUALE.LAYER_OUTPUT.TEMPLATE_ASSET_ENCUMBRANCE.DATI.DATA_POINT", "order": "asc"}], "unique": false, "multivalue": true}""";

			sodaCollection.admin().createIndex(db.createDocumentFrom(indexSpec));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
*/
//	public static void main(String[] args) {
//		String fileName = args[0];
//		File collectionMetadata = new File(fileName);
//
//		final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//		final SimpleModule indexKeyModule = new SimpleModule();
//		indexKeyModule.addDeserializer(MetadataKey.class, new MetadataKeyDeserializer());
//		mapper.registerModule(indexKeyModule);
//		try (InputStream inputStream = collectionMetadata.getName().toLowerCase().endsWith(".gz") ?
//				new GZIPInputStream(new FileInputStream(collectionMetadata), 16 * 1024)
//				: new BufferedInputStream(new FileInputStream(collectionMetadata), 16 * 1024)) {
//			MongoDBMetadata[] metadata = mapper.readValue(inputStream, MongoDBMetadata[].class);
//
//			boolean mongoDBAPICompatible = true;
//			int maxParallelDegree = 16;
//
//			System.out.println("ALTER SESSION ENABLE PARALLEL DDL");
//			int index = 0;
//			int ttlIndex = 0;
//			int spatialIndex = 0;
//			int mongoSearchIndex = 0;
//			int oracleSearchIndex = 0;
//
//			for (MongoDBMetadata m : metadata) {
//				String collectionName = m.getCollectionName();
//				System.out.println("-- Collection: " + collectionName);
//
//				boolean needSearchIndex = false;
//
//
//				for (MetadataIndex indexMetadata : m.getIndexes()) {
//					LOGGER.info(indexMetadata.toString());
//					if (indexMetadata.isTtl()) {
//						System.out.println("-- TTL index " + indexMetadata.getName() + " with data expiration after " + indexMetadata.getExpireAfterSeconds().value + " seconds");
//						ttlIndex++;
//						index++;
//					}
//					else if (indexMetadata.getName().contains("$**") || indexMetadata.getKey().text) {
//						needSearchIndex = true; //System.out.println("Need Search Index");
//						mongoSearchIndex++;
//					}
//					/*else if (indexMetadata.getName().equals("_id_")) {
//						// skipping primary key as it already exists now!
//						continue;
//					}*/
//					else {
//
//						boolean spatial = false;
//						String spatialColumn = "";
//						if (indexMetadata.getKey().spatial) {
//							spatialColumn = indexMetadata.getKey().columns.get(0).name;
//							spatial = true;
//						}
//
//						if (spatial) {
//							LOGGER.info("Spatial index");
//
//							boolean allDocsHavePoints = false;
//
//
//							final String SQLStatement = String.format(
//									"create index %s on %s " +
//											"(JSON_VALUE(" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ", '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
//											"indextype is MDSYS.SPATIAL_INDEX_V2" + (allDocsHavePoints ? " PARAMETERS ('layer_gtype=POINT cbtree_index=true')" : "") + (maxParallelDegree == -1 ? "" : " parallel " + maxParallelDegree), collectionName + "$" + indexMetadata.getName(), collectionName, spatialColumn);
//
//							LOGGER.info(SQLStatement);
//							long start = System.currentTimeMillis();
//							System.out.println(SQLStatement);
//							index++;
//							spatialIndex++;
//							LOGGER.info("Created spatial index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
//						}
//						else {
//							LOGGER.info("Normal index");
//							Map<String, FieldInfo> fieldsInfo = new HashMap<>();
//							final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + indexMetadata.getName(),
//									getCreateIndexColumns(collectionName, indexMetadata.getKey(), fieldsInfo), indexMetadata.isUnique());
//
//							LOGGER.info(indexSpec);
//							long start = System.currentTimeMillis();
//							System.out.println(indexSpec);
//							index++;
//							LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
//						}
//					}
//				}
//
//				if (needSearchIndex) {
//                    /*try (CallableStatement cs = c.prepareCall("{CALL CTX_DDL.SYNC_INDEX(idx_name => ?, memory => '512M', parallel_degree => ?, locking => CTX_DDL.LOCK_NOWAIT)}")) {
//                        cs.setString(1, collectionName + "$search_index");
//                        cs.setInt(2, maxParallelDegree);
//                        start = System.currentTimeMillis();
//                        cs.execute();
//                        System.out.println("Manual sync of Search Index done in " + getDurationSince(start));
//                    }*/
//
//					long start = System.currentTimeMillis();
//					System.out.println(String.format("CREATE SEARCH INDEX %s$search_index ON %s (" + (mongoDBAPICompatible ? "DATA" : "JSON_DOCUMENT") + ") FOR JSON PARAMETERS('DATAGUIDE OFF SYNC(every \"freq=secondly;interval=1\" MEMORY 2G parallel %d)')", collectionName, collectionName, maxParallelDegree));
//					LOGGER.info("Created Search Index (every 1s sync) in " + getDurationSince(start));
//					index++;
//					oracleSearchIndex++;
//				}
//
//				LOGGER.info("OK");
//			}
//
//			System.out.println("ALTER SESSION DISABLE PARALLEL DDL");
//			System.out.println("-- Total number of indexes: " + index);
//			System.out.println("-- . including spatial indexes: " + spatialIndex);
//			System.out.println("-- . including search indexes: " + oracleSearchIndex + (oracleSearchIndex < mongoSearchIndex ? " replacing " + mongoSearchIndex + " MongoDB search index(es)" : ""));
//			System.out.println("-- . including TTL indexes: " + ttlIndex);
//
//
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
