package com.oracle.mongo2ora.migration.oracle;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
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

import static com.oracle.mongo2ora.util.Tools.getDurationSince;

public class OracleCollectionInfo {
	private static final Logger LOGGER = Loggers.getLogger("index");

	private final String user;
	private final String collectionName;
	private final boolean autonomousDatabase;
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
		this.autonomousDatabase = autonomousDatabase;
	}


	public static OracleCollectionInfo getCollectionInfoAndPrepareIt(PoolDataSource pds, PoolDataSource adminPDS, String user, String collectionName, boolean dropAlreadyExistingCollection,
																	 boolean autonomousDatabase, boolean useMemoptimizeForWrite, boolean mongoDBAPICompatible) throws SQLException, OracleException {
		final OracleCollectionInfo ret = new OracleCollectionInfo(user, collectionName, autonomousDatabase);

		try (Connection c = adminPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				s.execute("grant execute on CTX_DDL to " + user);
				s.execute("grant create job to " + user);
			}

			try (Connection userConnection = pds.getConnection()) {
				// SODA: ensure the collection do exists!
				final Properties props = new Properties();
				props.put("oracle.soda.sharedMetadataCache", "true");
				props.put("oracle.soda.localMetadataCache", "true");

				final OracleRDBMSClient cl = new OracleRDBMSClient(props);
				final OracleDatabase db = cl.getDatabase(userConnection);
				OracleCollection sodaCollection = db.openCollection(collectionName);


				ret.emptyDestinationCollection = true;

				if (sodaCollection == null) {
					LOGGER.info((mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection does not exist => creating it");
					sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection( db, collectionName ) : db.admin().createCollection(collectionName);
					if (sodaCollection == null) {
						throw new IllegalStateException("Can't create "+(mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection: " + collectionName);
					}
					if(useMemoptimizeForWrite) {
						configureSODACollectionForMemoptimizeForWrite(userConnection,collectionName,mongoDBAPICompatible);
					}
				}
				else {
					try (Statement s = userConnection.createStatement()) {
						try (ResultSet r = s.executeQuery("select count(ID) from " + collectionName + " where rownum = 1")) {
							if (r.next() && r.getInt(1) == 1) {
								// THERE IS AT LEAST ONE ROW!
								if (dropAlreadyExistingCollection) {
									LOGGER.warn((mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection does exist => dropping it (requested with --drop CLI argument)");
									sodaCollection.admin().drop();
									LOGGER.info((mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection does exist => re-creating it");
									// TODO manage contentColumn data type:
									// TODO 21c+ => JSON
									// TODO 19c => BLOB if not autonomous database and not mongodb api compatible
									// TODO 19c => BLOB OSON if autonomous database or not mongodb api compatible
									sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection( db, collectionName ) : db.admin().createCollection(collectionName);
									if (sodaCollection == null) {
										throw new IllegalStateException("Can't re-create "+(mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection: " + collectionName);
									}
									if(useMemoptimizeForWrite) {
										configureSODACollectionForMemoptimizeForWrite(userConnection,collectionName, mongoDBAPICompatible);
									}
								}
								else {
									// avoid migrating data into non empty destination collection (no conflict to manage)
									ret.emptyDestinationCollection = false;
									return ret;
								}
							}
							else {
								if (dropAlreadyExistingCollection) {
									LOGGER.warn((mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection does exist (with 0 row) => dropping it (requested with --drop CLI argument)");
									sodaCollection.admin().drop();
									LOGGER.info((mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection does exist => re-creating it");
									sodaCollection = mongoDBAPICompatible ? createMongoDBAPICompatibleCollection( db, collectionName ) : db.admin().createCollection(collectionName);
									if (sodaCollection == null) {
										throw new IllegalStateException("Can't re-create "+(mongoDBAPICompatible?"MongoDB API compatible":"SODA")+" collection: " + collectionName);
									}
									if(useMemoptimizeForWrite) {
										configureSODACollectionForMemoptimizeForWrite(userConnection,collectionName, mongoDBAPICompatible);
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
				p.setString(2, collectionName.toUpperCase());

				try (ResultSet r = p.executeQuery()) {
					while (r.next()) {
						final String constraintText = r.getString(2);
						final String condition = constraintText.toLowerCase();
						if (condition.contains("is json") && condition.contains("json_document")) {
							ret.isJsonConstraintName = r.getString(1);
							ret.isJsonConstraintEnabled = r.getString(3).equalsIgnoreCase("ENABLED");
							ret.isJsonConstraintText = constraintText;
							ret.isJsonConstraintIsOSONFormat = condition.contains("format oson");
							LOGGER.info("IS JSON constraint found: " + ret.isJsonConstraintName + " for collection " + collectionName);
							break;
						}
					}
				}

				if (ret.isJsonConstraintName != null) {
					LOGGER.info("Disabling Is JSON constraint: " + ret.isJsonConstraintName);
					p.execute("alter table " + user + "." + collectionName + " disable constraint " + ret.isJsonConstraintName);
				}
			}

			try (PreparedStatement p = c.prepareStatement("select index_name, status from all_indexes where table_owner=? and table_name=? and uniqueness='UNIQUE' and constraint_index='YES'")) {
				p.setString(1, user);
				p.setString(2, collectionName.toUpperCase());

				try (ResultSet r = p.executeQuery()) {
					if (r.next()) {
						ret.primaryKeyIndexName = r.getString(1);
						ret.primaryKeyIndexStatus = r.getString(2);
						LOGGER.info("Primary key found: " + ret.primaryKeyIndexName + " for collection " + collectionName + " with status " + ret.primaryKeyIndexStatus);
					}
				}

				if (ret.primaryKeyIndexName != null /*&& !autonomousDatabase*/) {
					LOGGER.info("Dropping primary key and associated index: " + ret.primaryKeyIndexName + " for collection " + collectionName);
					LOGGER.info("Running: " + "alter table " + user + "." + collectionName + " drop primary key drop index");
					p.execute("alter table " + user + "." + collectionName + " drop primary key drop index");
					LOGGER.info("Running: " + "alter table " + user + "." + collectionName + " drop primary key drop index DONE!");
				}
			}
		}

		return ret;
	}

	private static OracleCollection createMongoDBAPICompatibleCollection(OracleDatabase db, String collectionName) throws SQLException, OracleException {
			try (CallableStatement cs = db.admin().getConnection().prepareCall("{call DBMS_SODA_ADMIN.CREATE_COLLECTION(P_URI_NAME => ?, P_CREATE_MODE => 'NEW', P_DESCRIPTOR => ?, P_CREATE_TIME => ?) }")) {
				final String metadata = """
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

				cs.registerOutParameter(3, Types.VARCHAR);
				cs.setString(1,collectionName);
				cs.setString(2, metadata);

				cs.execute();
			}

		return db.openCollection(collectionName);
	}

	private static void configureSODACollectionForMemoptimizeForWrite(Connection userConnection, String collectionName, boolean mongoDBAPICompatible) throws SQLException {
		try (PreparedStatement p = userConnection.prepareStatement("insert into "+collectionName+"(ID,VERSION,"+(mongoDBAPICompatible?"DATA":"JSON_DOCUMENT")+") values (?,?,?)")) {
			p.setString(1,"dummy");
			p.setString(2,"dummy");
			p.setString(3,"{}");
			p.executeUpdate();
			userConnection.commit();
		}

		try (PreparedStatement p = userConnection.prepareStatement("delete from "+collectionName+" where id=?")) {
			p.setString(1,"dummy");
			p.executeUpdate();
			userConnection.commit();
		}

		try (Statement s = userConnection.createStatement()) {
			s.execute("alter table "+collectionName+" memoptimize for write");
		}
	}

	/**
	 * Using medium service reconfigured with standard user.
	 *
	 * @param mediumPDS
	 * @param mongoCollection
	 * @throws SQLException
	 */
	public void finish(PoolDataSource mediumPDS, MongoCollection<Document> mongoCollection, int maxParallelDegree, ASCIIGUI gui, boolean mongoDBAPICompatible) throws SQLException, OracleException {
		try (Connection c = mediumPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				LOGGER.info("Enabling JSON constraint...");
				if (isJsonConstraintName != null) {
					LOGGER.info("Re-Enabling Is JSON constraint NOVALIDATE");
					s.execute("alter table " + collectionName + " modify constraint " + isJsonConstraintName + " enable novalidate");
				}
				else {
					LOGGER.info("Creating Is JSON constraint NOVALIDATE");
					s.execute("alter table " + collectionName + " add constraint " + collectionName + "_jd_is_json check ("+(mongoDBAPICompatible?"data":"json_document")+" is json format oson (size limit 32m)) novalidate");
				}
				LOGGER.info("OK");

				gui.startIndex("primary key");

				LOGGER.info("Adding primary key constraint and index...");
				// MOS 473656.1
				s.execute("ALTER SESSION ENABLE PARALLEL DDL");
				long start = System.currentTimeMillis();
				if (primaryKeyIndexName != null) {
					String currentPKIndexStatus = "?";
					try (PreparedStatement p = c.prepareStatement("select status from all_indexes where table_owner=? and table_name=? and index_name=? and uniqueness='UNIQUE' and constraint_index='YES'")) {
						p.setString(1, user);
						p.setString(2, collectionName.toUpperCase());
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
							LOGGER.info("CREATE UNIQUE INDEX " + user + "." + primaryKeyIndexName + " ON " + user + "." + collectionName + "(ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							s.execute("CREATE UNIQUE INDEX " + user + "." + primaryKeyIndexName + " ON " + user + "." + collectionName + "(ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
							LOGGER.info("ALTER TABLE " + user + "." + collectionName + " ADD CONSTRAINT " + primaryKeyIndexName + " PRIMARY KEY (ID) USING INDEX " + user + "." + primaryKeyIndexName + " ENABLE NOVALIDATE");
							s.execute("ALTER TABLE " + user + "." + collectionName + " ADD CONSTRAINT " + primaryKeyIndexName + " PRIMARY KEY (ID) USING INDEX " + user + "." + primaryKeyIndexName + " ENABLE NOVALIDATE");
							LOGGER.info("Created PK constraint and index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
						}
					}
				}
				else {
					try {
						LOGGER.info("CREATE UNIQUE INDEX " + "PK_" + collectionName + " ON " + collectionName + "(ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
						s.execute("CREATE UNIQUE INDEX " + "PK_" + collectionName + " ON " + collectionName + "(ID) PARALLEL" + (maxParallelDegree == -1 ? "" : " " + maxParallelDegree));
						LOGGER.info("ALTER TABLE " + collectionName + " ADD CONSTRAINT PK_" + collectionName + " PRIMARY KEY (ID) USING INDEX " + "PK_" + collectionName + " ENABLE NOVALIDATE");
						s.execute("ALTER TABLE " + collectionName + " ADD CONSTRAINT PK_" + collectionName + " PRIMARY KEY (ID) USING INDEX " + "PK_" + collectionName + " ENABLE NOVALIDATE");
						LOGGER.info("Created PK constraint and index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
					}
					catch (SQLException sqle) {
						sqle.printStackTrace();
					}
				}

				gui.endIndex("primary key");

				// manage other MongoDB indexes
				int mongoDBIndex = 0;

				if(mongoCollection != null) {
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

						try (ResultSet r = s.executeQuery("with dg as (select json_object( 'dg' : json_dataguide( json_document, dbms_json.format_flat, DBMS_JSON.GEOJSON+DBMS_JSON.GATHER_STATS) format JSON returning clob) as json_document from " + collectionName + " where rownum <= 1000)\n" +
								"select u.field_path, type, length from dg nested json_document columns ( nested dg[*] columns (field_path path '$.\"o:path\"', type path '$.type', length path '$.\"o:length\"', low path '$.\"o:low_value\"' )) u")) {
							while (r.next()) {
								String key = r.getString(1);
								if (!r.wasNull()) {
									fieldsInfo.put(key, new FieldInfo(key, r.getString(2), r.getInt(3)));
								}
							}
						}

						boolean needSearchIndex = false;

						for (Document indexMetadata : mongoCollection.listIndexes()) {
							LOGGER.info(indexMetadata.toString());
							if (indexMetadata.getString("name").contains("$**") || "text".equals(indexMetadata.getEmbedded(Arrays.asList("key"), Document.class).getString("_fts"))) {
								needSearchIndex = true; //System.out.println("Need Search Index");
							}
							else if (indexMetadata.getString("name").equals("_id_")) {
								// skipping primary key as it already exists now!
								continue;
							}
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
											"create index %s on %s " +
													"(JSON_VALUE("+(mongoDBAPICompatible?"DATA":"JSON_DOCUMENT")+", '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
													"indextype is MDSYS.SPATIAL_INDEX_V2" + (allDocsHavePoints ? " PARAMETERS ('layer_gtype=POINT cbtree_index=true')" : "") + (maxParallelDegree == -1 ? "" : " parallel " + maxParallelDegree), collectionName + "$" + indexMetadata.getString("name"), collectionName, spatialColumn);

									LOGGER.info(SQLStatement);
									start = System.currentTimeMillis();
									gui.startIndex(indexMetadata.getString("name"));
									s.execute(SQLStatement);
									LOGGER.info("Created spatial index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
									gui.endIndex(indexMetadata.getString("name"));
								}
								else {
									LOGGER.info("Normal index");
									final String indexSpec = String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + indexMetadata.getString("name"), getCreateIndexColumns(collectionName, keys, fieldsInfo), indexMetadata.getBoolean("unique"));

									LOGGER.info(indexSpec);
									start = System.currentTimeMillis();
									gui.startIndex(indexMetadata.getString("name"));
									sodaCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
									LOGGER.info("Created standard SODA index with parallel degree of " + maxParallelDegree + " in " + getDurationSince(start));
									gui.endIndex(indexMetadata.getString("name"));
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

							start = System.currentTimeMillis();
							gui.startIndex("search_index");
							s.execute(String.format("CREATE SEARCH INDEX %s$search_index ON %s ("+(mongoDBAPICompatible?"DATA":"JSON_DOCUMENT")+") FOR JSON PARAMETERS('DATAGUIDE OFF SYNC(every \"freq=secondly;interval=1\" MEMORY 2G parallel %d)')", collectionName, collectionName, maxParallelDegree));
							LOGGER.info("Created Search Index (every 1s sync) in " + getDurationSince(start));
							gui.endIndex("search_index");
						}

						s.execute("ALTER SESSION DISABLE PARALLEL DDL");
						LOGGER.info("OK");
					}
				}
			}
		}
		catch (SQLException sqle) {
			LOGGER.error("During finish step of collection " + collectionName, sqle);
			throw sqle;
		}
	}

	private String getCreateIndexColumns(String collectionName, Document keys, Map<String, FieldInfo> fieldsInfo) {
		final StringBuilder s = new StringBuilder();

		for (String columnName : keys.keySet()) {
			if (s.length() > 0) {
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

			if (fi == null) {
				s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\"}");
			}
			else {
				s.append("{\"path\": \"").append(columnName).append("\", \"order\": \"").append(keys.getInteger(columnName) == 1 ? "asc" : "desc").append("\", \"datatype\": \"").append(fi.type).append("\"");
				if ("string".equals(fi.type)) {
					s.append(", \"maxlength\": ").append(fi.length);
				}
				s.append("}");
			}

		}

		return s.toString();
	}

	public void dropSODACollection(PoolDataSource pds) throws SQLException, OracleException {
		try (Connection userConnection = pds.getConnection()) {
			final Properties props = new Properties();
			props.put("oracle.soda.sharedMetadataCache", "true");
			props.put("oracle.soda.localMetadataCache", "true");

			final OracleRDBMSClient cl = new OracleRDBMSClient(props);
			final OracleDatabase db = cl.getDatabase(userConnection);
			final OracleCollection sodaCollection = db.openCollection(collectionName);
			sodaCollection.admin().drop();
		}
	}

	static class FieldInfo {
		public String path;
		public String type;
		public int length;

		public FieldInfo(String path, String type, int length) {
			this.path = path;
			this.type = type;
			this.length = length;
		}
	}
}
