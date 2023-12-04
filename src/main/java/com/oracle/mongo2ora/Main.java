package com.oracle.mongo2ora;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.Configuration;
import com.oracle.mongo2ora.migration.ConversionInformation;
import com.oracle.mongo2ora.migration.converter.BSON2TextCollectionConverter;
import com.oracle.mongo2ora.migration.converter.DirectDirectPathBSON2OSONCollectionConverter;
import com.oracle.mongo2ora.migration.mongodb.CollectionCluster;
import com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer;
import com.oracle.mongo2ora.migration.mongodb.MongoDatabaseDump;
import com.oracle.mongo2ora.migration.oracle.MediumServiceManager;
import com.oracle.mongo2ora.migration.oracle.OracleAutoTasks;
import com.oracle.mongo2ora.migration.oracle.OracleCollectionInfo;
import com.oracle.mongo2ora.reporting.IndexReport;
import com.oracle.mongo2ora.reporting.LoadingReport;
import com.oracle.mongo2ora.util.Kernel32;
import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.Native;
import net.rubygrapefruit.platform.terminal.Terminals;
import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static com.mongodb.client.model.Projections.include;
import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;
import static com.oracle.mongo2ora.util.XYTerminalOutput.*;
import static java.util.stream.Collectors.toList;

/**
 * TODO:
 * - summary at the end
 *   - add real table size
 * - add index size
 * - check free space before copying
 * select
 *    fs.tablespace_name                          "Tablespace",
 *    (df.totalspace - fs.freespace)              "Used MB",
 *    fs.freespace                                "Free MB",
 *    df.totalspace                               "Total MB",
 *    round(100 * (fs.freespace / df.totalspace)) "Pct. Free"
 * from
 *    (select
 *       tablespace_name,
 *       round(sum(bytes) / 1048576) TotalSpace
 *    from
 *       dba_data_files
 *    group by
 *       tablespace_name
 *    ) df,
 *    (select
 *       tablespace_name,
 *       round(sum(bytes) / 1048576) FreeSpace
 *    from
 *       dba_free_space
 *    group by
 *       tablespace_name
 *    ) fs
 * where
 *    df.tablespace_name = fs.tablespace_name;
 * <p>
 * OK display index creation
 * OK work on Autonomous database detection and adapt with OSON support
 * - work on Autonomous database detection and adapt with JSON datatype native support
 * - help migration using properties file for per collection configuration (range partitioning, SODA collection columns, types etc..., filtering...)
 */
public class Main {
	public static final String VERSION = "1.3.0";

	private static final Logger LOGGER = Loggers.getLogger("main");

	public static boolean ENABLE_COLORS = true;

	public static final int WANTED_THREAD_PRIORITY = Thread.NORM_PRIORITY + 1;

	public static XYTerminalOutput TERM;

	static ExecutorService workerThreadPool;
	static ExecutorService counterThreadPool;
	static int MONGODB_COLLECTIONS = 0;
	static int MONGODB_INDEXES = 0;

	public static ASCIIGUI gui;

	public static boolean AUTONOMOUS_DATABASE = false;
	public static int ORACLE_MAJOR_VERSION = -1;

	public static String MAX_STRING_SIZE = "STANDARD";
	public static String AUTONOMOUS_DATABASE_TYPE = "";

	private final static byte[] bsonDataSize = new byte[4];

	private static byte[] readNextBSONRawData(InputStream input) throws IOException {
		int readBytes = input.read(bsonDataSize, 0, 4);
		if (readBytes != 4) throw new EOFException();

		final int bsonSize = (bsonDataSize[0] & 0xff) |
				((bsonDataSize[1] & 0xff) << 8) |
				((bsonDataSize[2] & 0xff) << 16) |
				((bsonDataSize[3] & 0xff) << 24);

		previousPosition = position;
		position += bsonSize;

		final byte[] rawData = new byte[bsonSize];

		System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

		for (int i = bsonSize - 4, off = 4; i > 0; off += readBytes) {
			readBytes = input.read(rawData, off, i);
			if (readBytes < 0) {
				throw new EOFException();
			}

			i -= readBytes;
		}

		return rawData;
	}

	static long position = 0;
	static long previousPosition = 0;

	private static int skipNextBSONRawData(InputStream input) throws IOException {
		int readBytes = input.read(bsonDataSize, 0, 4);
		if (readBytes != 4) throw new EOFException();

		final int bsonSize = (bsonDataSize[0] & 0xff) |
				((bsonDataSize[1] & 0xff) << 8) |
				((bsonDataSize[2] & 0xff) << 16) |
				((bsonDataSize[3] & 0xff) << 24);

		previousPosition = position;
		position += bsonSize;

		long skeptBytes;
		for (int i = bsonSize - 4; i > 0; ) {
			skeptBytes = input.skip(i);
			if (skeptBytes < 0) {
				throw new EOFException();
			}

			i -= skeptBytes;
		}

		return bsonSize;
	}

	private static void initialize(Configuration conf) {
		LOGGER.info("===========================================================================================================");
		LOGGER.info("mongo2ora v" + VERSION + " started!");
		Security.setProperty("networkaddress.cache.ttl", "0"); // For Autonomous Database CMAN load balancing
		System.setProperty("oracle.jdbc.fanEnabled", "false");

		Locale.setDefault(Locale.US);

		// will run only on Windows OS
		Kernel32.init();

		final Terminals TERMINALS = Native.get(Terminals.class).withAnsiOutput();
		if (!TERMINALS.isTerminal(Terminals.Output.Stdout)) {
			return;
		}

		TERM = new XYTerminalOutput(TERMINALS.getTerminal(Terminals.Output.Stdout));

		gui = new ASCIIGUI(TERM, "--== " +
				BOLD + BrightGreen + "Mongo" +
				BrightYellow + "2" +
				BrightRed + "Ora" + RESET
				+ " v" + VERSION + " - the MongoDB to Oracle migration tool ==--" + RESET);

		gui.setSourceDatabaseName(conf.sourceDatabase);
		gui.setDestinationDatabaseName(conf.destinationUsername);
		gui.start();

		//----------------------------------------------------------------------------------------------
		// PREPARING THREAD POOLS
		//----------------------------------------------------------------------------------------------
		//LOGGER.info("COUNTER THREADS=" + Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors()));
		counterThreadPool = Executors.newVirtualThreadPerTaskExecutor();
		LOGGER.info("WORKER THREADS=" + conf.cores);
		workerThreadPool = counterThreadPool;
	}

	public static final LoadingReport REPORT = new LoadingReport();

	public static void main(final String[] args) {
		try {
			// Create configuration related to command line args
			final Configuration conf = Configuration.prepareConfiguration(args);

			initialize(conf);

			if (conf.sourceDump) {
				REPORT.source = "MongoDB dump";
				REPORT.sourceDumpFolder = conf.sourceDumpFolder;
				loadAllCollectionsFromDumpFiles(conf);
			}
			else {
				REPORT.source = "MongoDB database";
				loadAllCollectionsFromDatabase(conf);
			}
		}
		finally {
			LOGGER.info(REPORT.toString());
			TERM.write("A report is available inside mongo2ora.log");
		}
	}

	private static void loadAllCollectionsFromDatabase(Configuration conf) {
		// Connect to MongoDB database
		final MongoClientSettings settings = conf.sourceUsername == null || conf.sourceUsername.isEmpty() ?
				MongoClientSettings.builder()
						.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
						.applyToConnectionPoolSettings(builder ->
								builder.maxSize(conf.cores).minSize(conf.cores).maxConnecting(conf.cores).maxConnectionIdleTime(10, TimeUnit.MINUTES))
						.applyToClusterSettings(builder ->
								builder.hosts(Arrays.asList(new ServerAddress(conf.sourceHost, conf.sourcePort))))
						.build()
				:
				MongoClientSettings.builder()
						.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
						.credential(MongoCredential.createCredential(conf.sourceUsername, conf.sourceDatabase, conf.sourcePassword.toCharArray()))
						.applyToConnectionPoolSettings(builder ->
								builder.maxSize(conf.cores).minSize(conf.cores).maxConnecting(conf.cores).maxConnectionIdleTime(10, TimeUnit.MINUTES))
						.applyToClusterSettings(builder ->
								builder.hosts(Arrays.asList(new ServerAddress(conf.sourceHost, conf.sourcePort))))
						.build();

		try (MongoClient mongoClient = MongoClients.create(settings)) {
			final PoolDataSource pds = initializeConnectionPool(false, conf.destinationConnectionString, conf.destinationUsername, conf.destinationPassword, conf.cores);
			final PoolDataSource adminPDS = initializeConnectionPool(true, conf.destinationConnectionString, conf.destinationAdminUser, conf.destinationAdminPassword, 3);
			conf.initializeMaxParallelDegree(adminPDS);
			gui.setPDS(adminPDS);

			// Get destination database information
			displayOracleDatabaseVersion(adminPDS);

			// Grant create job and CTX_DDL for managing Search indexes...
			grantPrivilegesToDestinationUser(conf, adminPDS);

			MongoDatabase mongoDatabase = mongoClient.getDatabase(conf.sourceDatabase);
			final Document result = mongoDatabase.runCommand(new Document("buildInfo", 1));
			gui.setsourceDatabaseVersion((String) result.get("version"));

			// get number of collections and indexes in this database
			MONGODB_COLLECTIONS = MONGODB_INDEXES = 0;
			for (Document d : mongoDatabase.listCollections()) {
				if (conf.selectedCollections.isEmpty() || conf.selectedCollections.contains(d.getString("name"))) {
					MONGODB_COLLECTIONS++;

					final MongoCollection<Document> collection = mongoDatabase.getCollection(d.getString("name"));
					for (Document i : collection.listIndexes()) {
						MONGODB_INDEXES++;
					}
				}
			}
			gui.setNumberOfMongoDBCollections(MONGODB_COLLECTIONS);
			gui.setNumberOfMongoDBIndexes(MONGODB_INDEXES);
			gui.setNumberOfMongoDBJSONDocuments(0);
			gui.setTotalMongoDBSize(0);

			REPORT.numberOfCollections = MONGODB_COLLECTIONS;
			REPORT.numberOfIndexes = MONGODB_INDEXES;

			// Disabling Automatic ADB-S tasks if any
			OracleAutoTasks.disableIfNeeded(adminPDS);

			final PoolDataSource mediumPDS = MediumServiceManager.configure(adminPDS, pds, conf.destinationPassword);

			final Semaphore DB_SEMAPHORE = new Semaphore(conf.cores);

			for (Document collectionDescription : mongoDatabase.listCollections()) {
				final String collectionName = collectionDescription.getString("name");

				if (!conf.selectedCollections.isEmpty() && !conf.selectedCollections.contains(collectionName)) {
					//System.out.println("Collection " + collectionName + " will not be migrated.");
					continue;
				}

				final long startTimeCollection = System.currentTimeMillis();

				REPORT.addCollection(collectionName, conf.samples != -1);

				final MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);

				// disable is JSON constraint, remove indexes...
				final OracleCollectionInfo oracleCollectionInfo = OracleCollectionInfo.getCollectionInfoAndPrepareIt(pds, adminPDS, conf, collectionName, AUTONOMOUS_DATABASE);

				if (!oracleCollectionInfo.emptyDestinationCollection) {
					LOGGER.warn("Collection " + collectionName + " will not be migrated because destination is not empty!");
					continue;
				}

				gui.addNewDestinationDatabaseCollection(collectionName, mongoCollection, null);

				if (!conf.buildSecondaryIndexes) {
					// retrieve average document size
					final Iterable<Document> statsIterator = mongoCollection.aggregate(Arrays.asList(
							new BsonDocument("$collStats", new BsonDocument("storageStats", new BsonDocument("scale", new BsonInt32(1))))
					));
					final Document stats = (Document) statsIterator.iterator().next().get("storageStats");
					final long averageDocumentSize = stats.getInteger("avgObjSize");

					// Run clusterization on collection
					Document min = mongoCollection.find().projection(include("_id")).sort(new Document("_id", 1)).hint(useIdIndexHint).first();
					Document max = mongoCollection.find().projection(include("_id")).sort(new Document("_id", -1)).hint(useIdIndexHint).first();

					//System.out.println("min _id: " + min.get("_id").toString());
					//System.out.println("max _id: " + max.get("_id").toString());

					final long minId = Long.parseLong(Objects.requireNonNull(min).get("_id").toString().substring(0, 8), 16);
					final long maxId = Long.parseLong(Objects.requireNonNull(max).get("_id").toString().substring(0, 8), 16);

					//System.out.println("min 8 first chars _id: " + minId);
					//System.out.println("max 8 first chars _id: " + maxId);

					long bucketSize = 8L;

					long iterations = ((maxId - minId) / bucketSize) + 1;

					// We don't want to start 100000+ threads!
					while (iterations > 100000) {
						bucketSize *= 2L;
						iterations = ((maxId - minId) / bucketSize) + 1;
					}

					//System.out.println(collectionName + ": bucket size= " + bucketSize + ", iterations: " + iterations);

					final List<CompletableFuture<CollectionCluster>> publishingCfs = new LinkedList<>();

					long tempMin = minId;
					final long startClusterAnalysis = System.currentTimeMillis();

					for (long i = 0; i < iterations; i++, tempMin += bucketSize) {

						final long _max = i == iterations - 1 ? maxId + 1 : tempMin + bucketSize;

						//System.out.println(i + " From " + tempMin + " to " + _max);
						//System.out.println(i + "\t=> from " + Long.toHexString(tempMin) + " to " + Long.toHexString(_max));

						final CompletableFuture<CollectionCluster> publishingCf = new CompletableFuture<>();
						publishingCfs.add(publishingCf);
						counterThreadPool.execute(new CollectionClusteringAnalyzer(i, collectionName, publishingCf, tempMin, _max, mongoDatabase, gui, averageDocumentSize));
					}

					final List<CompletableFuture<ConversionInformation>> publishingCfsConvert = new LinkedList<>();
					final List<CollectionCluster> mongoDBCollectionClusters = new ArrayList<>();

					long total = 0;
					int i = 0;
					final List<CollectionCluster> clusters = new ArrayList<>();

					// source == MongoDB database
					if (ORACLE_MAJOR_VERSION >= 23) {
						String IDproperty = conf.collectionsProperties.getProperty(collectionName + ".ID", "EMBEDDED_OID");

						if (/*"STANDARD".equalsIgnoreCase(MAX_STRING_SIZE) &&*/ "EMBEDDED_OID".equalsIgnoreCase(IDproperty)) {
							try (Connection c = pds.getConnection()) {
								try (Statement s = c.createStatement()) {
									s.execute("alter table \"" + oracleCollectionInfo.getTableName() + "\" drop column id");
								}
							}
						}
					}

					for (CompletableFuture<CollectionCluster> publishingCf : publishingCfs) {
						CollectionCluster cc = publishingCf.join();
						clusters.add(cc);

						if (cc.count > 0) {
							total += cc.count;
							mongoDBCollectionClusters.add(cc);

							final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
							publishingCfsConvert.add(pCf);

							workerThreadPool.execute(AUTONOMOUS_DATABASE || ORACLE_MAJOR_VERSION >= 21 || conf.mongodbAPICompatible || conf.forceOSON ?
									new DirectDirectPathBSON2OSONCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE, conf.mongodbAPICompatible, ORACLE_MAJOR_VERSION, conf.collectionsProperties, conf.allowDuplicateKeys, null) :
									new BSON2TextCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize));

							i++;
						}
					}

					//System.out.println("Docs: " + total + " for " + mongoDBCollectionClusters.size() + " cluster(s)");
					//println(Console.Style.ANSI_BLUE + "Collection clustering analysis duration: " + getDurationSince(startClusterAnalysis));

					clusters.clear();

					final List<ConversionInformation> informations = publishingCfsConvert.stream().map(CompletableFuture::join).collect(toList());

					for (ConversionInformation ci : informations) {
						if (ci.exception != null) {
							LOGGER.error("Error during ingestion!", ci.exception);
						}
					}

					// source == mongodump
					if (ORACLE_MAJOR_VERSION >= 23) {
						String IDproperty = conf.collectionsProperties.getProperty(collectionName + ".ID", "EMBEDDED_OID");

						if (/*"STANDARD".equalsIgnoreCase(MAX_STRING_SIZE) &&*/ "EMBEDDED_OID".equalsIgnoreCase(IDproperty)) {
							try (Connection c = pds.getConnection()) {
								try (Statement s = c.createStatement()) {
									s.execute("alter table \"" + oracleCollectionInfo.getTableName() + "\" add id AS (JSON_VALUE(\"DATA\" FORMAT OSON , '$._id' RETURNING ANY ORA_RAWCOMPARE NO ARRAY ERROR ON ERROR)) MATERIALIZED NOT NULL ENABLE");
								}
							}
						}
					}
				}

				// TODO: manage indexes (build parallel using MEDIUM service changed configuration)
				oracleCollectionInfo.finish(mediumPDS, mongoCollection, null, conf, gui, ORACLE_MAJOR_VERSION);
				//gui.finishCollection();

				computeOracleObjectSize(pds, oracleCollectionInfo);
			}

			gui.finishLastCollection();

			gui.stop();

			// Disabling Automatic ADB-S tasks if any
			OracleAutoTasks.enableIfNeeded(adminPDS);

			MediumServiceManager.restore(adminPDS);

			gui.flushTerminal();
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			if (counterThreadPool != null) {
				counterThreadPool.shutdown();
			}
			if (workerThreadPool != null) {
				workerThreadPool.shutdown();
			}
		}
	}

	private static void grantPrivilegesToDestinationUser(Configuration conf, PoolDataSource adminPDS) throws SQLException {
		try (Connection c = adminPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				s.execute("grant execute on CTX_DDL to " + conf.destinationUsername.toUpperCase());
			}
			catch (SQLException ignored) {
				LOGGER.warn("Unable to grant execute on CTX_DDL!");
			}

			try (Statement s = c.createStatement()) {
				s.execute("grant create job to " + conf.destinationUsername.toUpperCase());
			}
			catch (SQLException ignored) {
				LOGGER.warn("Unable to grant create job!");
			}
		}
	}

	private static void loadAllCollectionsFromDumpFiles(Configuration conf) {
		gui.setsourceDatabaseVersion("(dump)");

		try {
			final PoolDataSource pds = initializeConnectionPool(false, conf.destinationConnectionString, conf.destinationUsername, conf.destinationPassword, conf.cores);
			final PoolDataSource adminPDS = initializeConnectionPool(true, conf.destinationConnectionString, conf.destinationAdminUser, conf.destinationAdminPassword, 3);
			conf.initializeMaxParallelDegree(adminPDS);
			gui.setPDS(adminPDS);

			// Get destination database information
			displayOracleDatabaseVersion(adminPDS);

			// get number of collections and indexes in this database
			MONGODB_COLLECTIONS = MONGODB_INDEXES = 0;
			MongoDatabaseDump mongoDatabase = new MongoDatabaseDump(conf.sourceDumpFolder);
			for (String c : mongoDatabase.listCollectionsDump()) {
				if (conf.selectedCollections.isEmpty() || conf.selectedCollections.contains(c)) {
					MONGODB_COLLECTIONS++;
					MONGODB_INDEXES += mongoDatabase.getNumberOfIndexesForCollection(c);
				}
			}
			gui.setNumberOfMongoDBCollections(MONGODB_COLLECTIONS);
			gui.setNumberOfMongoDBIndexes(MONGODB_INDEXES);
			gui.setNumberOfMongoDBJSONDocuments(0);
			gui.setTotalMongoDBSize(0);

			REPORT.numberOfCollections = MONGODB_COLLECTIONS;
			REPORT.numberOfIndexes = MONGODB_INDEXES;

			// Disabling Automatic ADB-S tasks if any
			OracleAutoTasks.disableIfNeeded(adminPDS);

			final PoolDataSource mediumPDS = MediumServiceManager.configure(adminPDS, pds, conf.destinationPassword);

			final Semaphore DB_SEMAPHORE = new Semaphore(conf.cores);

			for (String collectionName : mongoDatabase.listCollectionsDump()) {
				if (!conf.selectedCollections.isEmpty() && !conf.selectedCollections.contains(collectionName)) {
					continue;
				}

				final long startTimeCollection = System.currentTimeMillis();

				REPORT.addCollection(collectionName, conf.samples != -1);

				// disable is JSON constraint, remove indexes...
				final OracleCollectionInfo oracleCollectionInfo = OracleCollectionInfo.getCollectionInfoAndPrepareIt(pds, adminPDS, conf, collectionName, AUTONOMOUS_DATABASE);

				if (!oracleCollectionInfo.emptyDestinationCollection) {
					LOGGER.warn("Collection " + collectionName + " will not be migrated because destination is not empty!");
					continue;
				}

				gui.addNewDestinationDatabaseCollection(collectionName, null, mongoDatabase.getCollectionMetadata(collectionName));

				if (!conf.buildSecondaryIndexes) {
					// THE WORK!
					loadCollectionDataFromDump(conf, collectionName, mongoDatabase, pds, oracleCollectionInfo, DB_SEMAPHORE);
				}

				// TODO: manage indexes (build parallel using MEDIUM service changed configuration)
				oracleCollectionInfo.finish(mediumPDS, null, mongoDatabase.getCollectionMetadata(collectionName), conf, gui, ORACLE_MAJOR_VERSION);
				//gui.finishCollection();

				computeOracleObjectSize(pds, oracleCollectionInfo);
			}

			gui.finishLastCollection();

			gui.stop();

			// Disabling Automatic ADB-S tasks if any
			OracleAutoTasks.enableIfNeeded(adminPDS);

			MediumServiceManager.restore(adminPDS);

			gui.flushTerminal();
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			if (counterThreadPool != null) {
				counterThreadPool.shutdown();
			}
			if (workerThreadPool != null) {
				workerThreadPool.shutdown();
			}
		}
	}

	private static void computeOracleObjectSize(PoolDataSource pds, OracleCollectionInfo oracleCollectionInfo) {
		try(Connection c = pds.getConnection()) {
			String lobSegmentName = null;
			String lobIndexName = null;
			try(PreparedStatement p = c.prepareStatement("select segment_name, index_name from USER_LOBS where table_name=?")) {
				p.setString(1,oracleCollectionInfo.getTableName());
				try(ResultSet r = p.executeQuery()) {
					if(r.next()) {
						lobSegmentName = r.getString(1);
						lobIndexName = r.getString(2);
					}
				}
			}

			long tableSize = 0;

			try(PreparedStatement p = c.prepareStatement("select sum(bytes) from user_segments where segment_name=?")) {
				p.setString(1,oracleCollectionInfo.getTableName());
				try(ResultSet r = p.executeQuery()) {
					if(r.next()) {
						tableSize = r.getLong(1);
					}
				}

				if(lobSegmentName != null) {
					p.setString(1,lobSegmentName);
					try(ResultSet r = p.executeQuery()) {
						if(r.next()) {
							tableSize += r.getLong(1);
						}
					}
				}

				if(lobIndexName != null) {
					p.setString(1,lobIndexName);
					try(ResultSet r = p.executeQuery()) {
						if(r.next()) {
							tableSize += r.getLong(1);
						}
					}
				}

				for(IndexReport ir : REPORT.getCollection(oracleCollectionInfo.getCollectionName()).indexes) {
					p.setString(1,ir.name);
					try(ResultSet r = p.executeQuery()) {
						if(r.next()) {
							ir.indexSize = r.getLong(1);
						}
					}

					if(ir.materializedViewName != null) {
						p.setString(1,ir.materializedViewName.toUpperCase());
						try(ResultSet r = p.executeQuery()) {
							if(r.next()) {
								ir.materializedViewSize = r.getLong(1);
							}
						}
					}
				}
			}

			REPORT.getCollection(oracleCollectionInfo.getCollectionName()).tableSize = tableSize;


		}
		catch(SQLException sqle) {
			LOGGER.error("While retrieving objects size in Oracle for "+oracleCollectionInfo.getCollectionName(),sqle);
		}
	}


	private static void loadCollectionDataFromGzippedDump(Configuration conf, String collectionName, MongoDatabaseDump mongoDatabase, PoolDataSource pds,
														  OracleCollectionInfo oracleCollectionInfo, Semaphore DB_SEMAPHORE, File bsonFile) throws IOException, SQLException {
		final List<CompletableFuture<ConversionInformation>> publishingCfsConvert = new LinkedList<>();

		long count = 0;
		long totalBSONSize = 0;
		position = previousPosition = 0;

		final Semaphore GUNZIP_SEMAPHORE = new Semaphore(conf.cores);

		try (InputStream inputStream = new GZIPInputStream(new FileInputStream(bsonFile), 128 * 1024 * 1024)) {
			long clusterStartPosition = 0;
			long clusterCount = 0;
			long totalCount = 0;

			// read until end of gzipped stream
			ByteBuffer buffer = ByteBuffer.allocateDirect((int)(conf.dumpBufferSize * 1024L * 1024L));

			int i = 0;
			while (true) {
				try {
					final byte[] data = readNextBSONRawData(inputStream);
					totalBSONSize += data.length;
					clusterCount++;
					totalCount++;

					if(buffer.remaining() >= data.length) {
						//LOGGER.info("Filling buffer...");
						buffer.put(data);
					} else {
						// overflow
						final int bufferPosition = buffer.position();
						buffer.rewind();
						final ByteBuffer bufferToWorkOn = ByteBuffer.allocateDirect(bufferPosition);
						bufferToWorkOn.put(0,buffer,0,bufferPosition).rewind();

						count += (--clusterCount);

						final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
						publishingCfsConvert.add(pCf);
						CollectionCluster cc = new CollectionCluster(clusterCount, clusterStartPosition, (int) (previousPosition - clusterStartPosition), bufferToWorkOn);

						try {
							GUNZIP_SEMAPHORE.acquire();
						}
						catch(InterruptedException e) {
							throw new RuntimeException(e);
						}

						workerThreadPool.execute(AUTONOMOUS_DATABASE || ORACLE_MAJOR_VERSION >= 21 || conf.mongodbAPICompatible || conf.forceOSON ?
						new DirectDirectPathBSON2OSONCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE, conf.mongodbAPICompatible, ORACLE_MAJOR_VERSION, conf.collectionsProperties, conf.allowDuplicateKeys, GUNZIP_SEMAPHORE) :
						new BSON2TextCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize));

						i++;
						// publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition, (int) (previousPosition - clusterStartPosition)));

						buffer.rewind();
						buffer.put(data);

						gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long) ((double) (previousPosition - clusterStartPosition) / (double) clusterCount));

						// prepare next set
						clusterCount = 1;
						clusterStartPosition = previousPosition;
					}

							if (conf.samples != -1 && totalCount >= conf.samples) {
								break;
							}
//						}
//					}
				}
				catch (EOFException eof) {
					break; // end of while(true)
				}
			}

			if (clusterCount > 0) {
//				final boolean sizeOverFlow = (position - clusterStartPosition) > conf.dumpBufferSize * 1024L * 1024L;
				count += clusterCount;
//				publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition, (int) (position - clusterStartPosition)));
				//LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");

				final int bufferPosition = buffer.position();
				buffer.rewind();
				final ByteBuffer bufferToWorkOn = ByteBuffer.allocateDirect(bufferPosition);
				bufferToWorkOn.put(0,buffer,0,bufferPosition).rewind();

				final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
				publishingCfsConvert.add(pCf);
				CollectionCluster cc = new CollectionCluster(clusterCount, clusterStartPosition, (int) (position - clusterStartPosition), bufferToWorkOn);

				workerThreadPool.execute(AUTONOMOUS_DATABASE || ORACLE_MAJOR_VERSION >= 21 || conf.mongodbAPICompatible || conf.forceOSON ?
						new DirectDirectPathBSON2OSONCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE, conf.mongodbAPICompatible, ORACLE_MAJOR_VERSION, conf.collectionsProperties, conf.allowDuplicateKeys, null) :
						new BSON2TextCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize));

				gui.updateSourceDatabaseDocuments(clusterCount, (long) ((double) (position - clusterStartPosition) / (double) clusterCount));
			}

			buffer = null;
		}

		LOGGER.info("Collection " + collectionName + " has " + count + " JSON document(s).");

		REPORT.getCollection(collectionName).totalDocumentsLoaded = count;
		REPORT.getCollection(collectionName).totalBSONSize = totalBSONSize;

		final List<ConversionInformation> informations = publishingCfsConvert.stream().map(CompletableFuture::join).collect(toList());

		for (ConversionInformation ci : informations) {
			if (ci.exception != null) {
				LOGGER.error("Error during ingestion!", ci.exception);
			}
		}
	}

	private static void loadCollectionDataFromRawDump(Configuration conf, String collectionName, MongoDatabaseDump mongoDatabase, PoolDataSource pds,
													  OracleCollectionInfo oracleCollectionInfo, Semaphore DB_SEMAPHORE, File bsonFile) throws IOException, SQLException {
		final List<CollectionCluster> publishingCfs = new LinkedList<>();

		long count = 0;
		long totalBSONSize = 0;
		position = previousPosition = 0;

		try (InputStream inputStream = new BufferedInputStream(new FileInputStream(bsonFile), 128 * 1024 * 1024)) {
			long clusterStartPosition = 0;
			long clusterCount = 0;
			long totalCount = 0;
			while (true) {
				try {
					totalBSONSize += skipNextBSONRawData(inputStream);
					clusterCount++;
					totalCount++;

					// limit cluster size to 100,000 documents or 128 MB
					boolean sizeOverFlow = false;
					if ((conf.samples != -1 && totalCount >= conf.samples) || (sizeOverFlow = ((position - clusterStartPosition) > conf.dumpBufferSize * 1024L * 1024L)) || clusterCount == 100000) {
						if (sizeOverFlow) {
							clusterCount--;
							count += clusterCount;
							publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition, (int) (previousPosition - clusterStartPosition)));
							//LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
							gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long) ((double) (previousPosition - clusterStartPosition) / (double) clusterCount));
							clusterCount = 1;
							clusterStartPosition = previousPosition;
						}
						else {
							count += clusterCount;
							publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition, (int) (position - clusterStartPosition)));
							//LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
							gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long) ((double) (position - clusterStartPosition) / (double) clusterCount));
							clusterCount = 0;
							clusterStartPosition = position;

							if (conf.samples != -1 && totalCount >= conf.samples) {
								break;
							}
						}
					}
				}
				catch (EOFException eof) {
					break;
				}
			}

			if (clusterCount > 0) {
				final boolean sizeOverFlow = (position - clusterStartPosition) > conf.dumpBufferSize * 1024L * 1024L;
				count += clusterCount;
				publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition, (int) (position - clusterStartPosition)));
				//LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
				gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long) ((double) (position - clusterStartPosition) / (double) clusterCount));
			}
		}

		LOGGER.info("Collection " + collectionName + " has " + count + " JSON document(s).");

		REPORT.getCollection(collectionName).totalDocumentsLoaded = count;
		REPORT.getCollection(collectionName).totalBSONSize = totalBSONSize;

		final List<CompletableFuture<ConversionInformation>> publishingCfsConvert = new LinkedList<>();
		final List<CollectionCluster> mongoDBCollectionClusters = new ArrayList<>();

		//long total = 0;
		int i = 0;
		final List<CollectionCluster> clusters = new ArrayList<>();

		for (CollectionCluster cc : publishingCfs) {
			clusters.add(cc);

			if (cc.count > 0) {
				//total += cc.count;
				mongoDBCollectionClusters.add(cc);

				final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
				publishingCfsConvert.add(pCf);

				workerThreadPool.execute(AUTONOMOUS_DATABASE || ORACLE_MAJOR_VERSION >= 21 || conf.mongodbAPICompatible || conf.forceOSON ?
						new DirectDirectPathBSON2OSONCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE, conf.mongodbAPICompatible, ORACLE_MAJOR_VERSION, conf.collectionsProperties, conf.allowDuplicateKeys, null) :
						new BSON2TextCollectionConverter(i % 256, oracleCollectionInfo.getCollectionName(), oracleCollectionInfo.getTableName(), cc, pCf, mongoDatabase, pds, gui, conf.batchSize));

				i++;
			}
		}

		clusters.clear();

		final List<ConversionInformation> informations = publishingCfsConvert.stream().map(CompletableFuture::join).collect(toList());

		for (ConversionInformation ci : informations) {
			if (ci.exception != null) {
				LOGGER.error("Error during ingestion!", ci.exception);
			}
		}
	}

	private static void loadCollectionDataFromDump(Configuration conf, String collectionName, MongoDatabaseDump mongoDatabase, PoolDataSource pds,
												   OracleCollectionInfo oracleCollectionInfo, Semaphore DB_SEMAPHORE) throws IOException, SQLException {
		// scan the BSON data
		// - compute averageDocumentSize
		// - split file into 5,000,000 BSON packets collection (denoting file position start)
		final File bsonFile = mongoDatabase.getBSONFile(collectionName);

		// source == mongodump
		if (ORACLE_MAJOR_VERSION >= 23) {
			String IDproperty = conf.collectionsProperties.getProperty(collectionName + ".ID", "EMBEDDED_OID");

			if (/*"STANDARD".equalsIgnoreCase(MAX_STRING_SIZE) &&*/ "EMBEDDED_OID".equalsIgnoreCase(IDproperty)) {
				try (Connection c = pds.getConnection()) {
					try (Statement s = c.createStatement()) {
						s.execute("alter table \"" + oracleCollectionInfo.getTableName() + "\" drop column id");
					}
				}
			}
		}

		if (bsonFile.getName().toLowerCase().endsWith(".gz")) {
			loadCollectionDataFromGzippedDump(conf, collectionName, mongoDatabase, pds, oracleCollectionInfo, DB_SEMAPHORE, bsonFile);
		}
		else {
			loadCollectionDataFromRawDump(conf, collectionName, mongoDatabase, pds, oracleCollectionInfo, DB_SEMAPHORE, bsonFile);
		}

		// source == mongodump
		if (ORACLE_MAJOR_VERSION >= 23) {
			String IDproperty = conf.collectionsProperties.getProperty(collectionName + ".ID", "EMBEDDED_OID");

			if (/*"STANDARD".equalsIgnoreCase(MAX_STRING_SIZE) &&*/ "EMBEDDED_OID".equalsIgnoreCase(IDproperty)) {
				LOGGER.info("Now recreating ID column...");
				try (Connection c = pds.getConnection()) {
					try (Statement s = c.createStatement()) {
						s.execute("alter table \"" + oracleCollectionInfo.getTableName() + "\" add id AS (JSON_VALUE(\"DATA\" FORMAT OSON , '$._id' RETURNING ANY ORA_RAWCOMPARE NO ARRAY ERROR ON ERROR)) MATERIALIZED NOT NULL ENABLE");
					}
				}
				LOGGER.info("ID column recreation OK");
			}
		}
	}

	private static void displayOracleDatabaseVersion(PoolDataSource adminPDS) {
		try (Connection c = adminPDS.getConnection()) {
			ORACLE_MAJOR_VERSION = c.getMetaData().getDatabaseMajorVersion();

			try (Statement s = c.createStatement()) {
				try(ResultSet r = s.executeQuery("select value from v$parameter where name='max_string_size'")) {
					if(r.next()) {
						MAX_STRING_SIZE = r.getString(1);
						LOGGER.info("max_string_size="+MAX_STRING_SIZE);
					}
				}

				try (ResultSet r = s.executeQuery("select version_full, count(*) from gv$instance group by version_full")) {
					if (r.next()) {
						final String oracleVersion = r.getString(1);
						int pos = oracleVersion.indexOf('.');
						pos = oracleVersion.indexOf('.', pos + 1);
						gui.setDestinationDatabaseVersion(oracleVersion.substring(0, pos));
						gui.setDestinationDatabaseInstances(r.getInt(2));

						REPORT.oracleVersion = oracleVersion.substring(0, pos);
						REPORT.oracleInstanceNumber = r.getInt(2);
					}
				}

				try (ResultSet r = s.executeQuery("select p.name, t.region, t.base_size, t.service, t.infrastructure from v$pdbs p, JSON_TABLE(p.cloud_identity, '$' COLUMNS (region path '$.REGION', base_size number path '$.BASE_SIZE', service path '$.SERVICE', infrastructure path '$.INFRASTRUCTURE')) t")) {
					if (r.next()) {
						AUTONOMOUS_DATABASE = true;
						AUTONOMOUS_DATABASE_TYPE = r.getString(4);
						if ("Shared".equalsIgnoreCase(r.getString(5))) {
							AUTONOMOUS_DATABASE_TYPE += "-S";
						}
						else {
							AUTONOMOUS_DATABASE_TYPE += "-D";
						}
						gui.setDestinationDatabaseType(AUTONOMOUS_DATABASE_TYPE);
						//setDBName(r.getString(1));
						//setRegion(r.getString(2));
						//setBaseSize(r.getLong(3)/1024d/1024d/1024d);
						REPORT.oracleDatabaseType = "Oracle Autonomous "+r.getString(5)+" ("+AUTONOMOUS_DATABASE_TYPE+")";
					}
				}
				catch (SQLException ignored) {
					AUTONOMOUS_DATABASE = false;
				}
			}
		}
		catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	private static PoolDataSource initializeConnectionPool(boolean admin, String ajdConnectionService, String user, String password, int cores) throws SQLException, IOException {
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		//pds.setConnectionFactoryClassName("oracle.jdbc.datasource.impl.OracleDataSource");

		if (ajdConnectionService.toLowerCase().trim().startsWith("(")) {
			pds.setURL("jdbc:oracle:thin:@" + ajdConnectionService);
		}
		else {
			pds.setURL("jdbc:oracle:thin:@" + ajdConnectionService + "?TNS_ADMIN=" + new File("wallet").getCanonicalPath().replace('\\', '/'));
		}

		pds.setUser(user);
		pds.setPassword(password);
		pds.setConnectionPoolName((admin ? "ADMIN_" : "") + "JDBC_UCP_POOL-" + Thread.currentThread().getName());
		pds.setInitialPoolSize(cores);
		pds.setMinPoolSize(cores);
		pds.setMaxPoolSize(cores);
		pds.setTimeoutCheckInterval(15 * 60);
		pds.setInactiveConnectionTimeout(15 * 60);
		pds.setValidateConnectionOnBorrow(!admin);
		pds.setMaxStatements(5);
		//pds.setMaxConnectionReuseTime(900);
		//pds.setMaxConnectionReuseCount(5000);
		//pds.setConnectionValidationTimeout();
		//pds.setSecondsToTrustIdleConnection(1);
		pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_ENABLE_AC_SUPPORT, "false");
		pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
		pds.setConnectionProperty("tcp.nodelay", "yes");
		pds.setConnectionProperty("oracle.jdbc.bindUseDBA", "true");
		pds.setConnectionProperty("oracle.jdbc.thinForceDNSLoadBalancing", "true");


		final Connection[] cons = new Connection[cores];

		for (int i = 0; i < cons.length; i++) {
			cons[i] = pds.getConnection();
			cons[i].setAutoCommit(false);
		}

		for (int i = 0; i < cons.length; i++) {
			cons[i].close();
		}
		return pds;
	}
}
