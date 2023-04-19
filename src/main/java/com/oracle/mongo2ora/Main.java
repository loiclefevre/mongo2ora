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
import com.oracle.mongo2ora.migration.converter.MemoptimizeForWriteBSON2OSONCollectionConverter;
import com.oracle.mongo2ora.migration.converter.RSIBSON2OSONCollectionConverter;
import com.oracle.mongo2ora.migration.converter.RSIBSON2TextCollectionConverter;
import com.oracle.mongo2ora.migration.mongodb.CollectionCluster;
import com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer;
import com.oracle.mongo2ora.migration.mongodb.MongoDatabaseDump;
import com.oracle.mongo2ora.migration.oracle.MediumServiceManager;
import com.oracle.mongo2ora.migration.oracle.OracleAutoTasks;
import com.oracle.mongo2ora.migration.oracle.OracleCollectionInfo;
import com.oracle.mongo2ora.util.Kernel32;
import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.Native;
import net.rubygrapefruit.platform.terminal.Terminals;
import oracle.jdbc.internal.OracleConnection;
import oracle.rsi.ReactiveStreamsIngestion;
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
import java.security.Security;
import java.sql.Connection;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
	public static final String VERSION = "1.2.0";

	private static final Logger LOGGER = Loggers.getLogger("main");

	public static boolean ENABLE_COLORS = true;

	public static final int WANTED_THREAD_PRIORITY = Thread.NORM_PRIORITY + 1;

	public static XYTerminalOutput TERM;

	static ExecutorService workerThreadPool;
	static ExecutorService counterThreadPool;

	static ExecutorService rsiWorkerThreadPool;
	static ReactiveStreamsIngestion rsi;

	static int MONGODB_COLLECTIONS = 0;
	static int MONGODB_INDEXES = 0;

	public static ASCIIGUI gui;

	public static boolean AUTONOMOUS_DATABASE = false;
	public static String AUTONOMOUS_DATABASE_TYPE = "";

	private final static byte[] bsonDataSize = new byte[4];

	private static byte[] readNextBSONRawData(InputStream input) throws IOException {
		int readBytes = input.read(bsonDataSize, 0, 4);
		if (readBytes != 4) throw new EOFException();

		final int bsonSize = (bsonDataSize[0] & 0xff) |
				((bsonDataSize[1] & 0xff) << 8) |
				((bsonDataSize[2] & 0xff) << 16) |
				((bsonDataSize[3] & 0xff) << 24);

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
	private static void skipNextBSONRawData(InputStream input) throws IOException {
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
	}

	public static void main(final String[] args) {
		// For Autonomous Database CMAN load balancing
		Security.setProperty("networkaddress.cache.ttl", "0");
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

		// Create configuration related to command line args
		final Configuration conf = Configuration.prepareConfiguration(args);
		//conf.println();

		gui.setSourceDatabaseName(conf.sourceDatabase);
		gui.setDestinationDatabaseName(conf.destinationUsername);
		gui.start();

		if (conf.useRSI) {
			LOGGER.info("RSI threads: " + conf.RSIThreads);
			rsiWorkerThreadPool = Executors.newFixedThreadPool(conf.RSIThreads
					,
					new ThreadFactory() {
						private final AtomicInteger threadNumber = new AtomicInteger(0);
						private final ThreadGroup group = new ThreadGroup("MongoDBMigration");

						@Override
						public Thread newThread(Runnable r) {
							final Thread t = new Thread(group, r,
									String.valueOf(threadNumber.getAndIncrement()),
									0);
							if (t.isDaemon())
								t.setDaemon(false);
							if (t.getPriority() != WANTED_THREAD_PRIORITY)
								t.setPriority(WANTED_THREAD_PRIORITY);
							return t;
						}
					});
		}

		// Connect to MongoDB database
		MongoCredential credential = null;
		if (!conf.sourceDump) {
			credential = MongoCredential.createCredential(conf.sourceUsername, conf.sourceDatabase, conf.sourcePassword.toCharArray());
		}

		MongoClientSettings settings = null;

		if (!conf.sourceDump) {
			settings = conf.sourceUsername == null || conf.sourceUsername.isEmpty() ?
					MongoClientSettings.builder()
							.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
							.applyToConnectionPoolSettings(builder ->
									builder.maxSize(conf.cores).minSize(conf.cores).maxConnecting(conf.cores).maxConnectionIdleTime(10, TimeUnit.MINUTES))
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(new ServerAddress(conf.sourceHost, conf.sourcePort))))
							.build()
					: MongoClientSettings.builder()
					.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
					.credential(credential)
					.applyToConnectionPoolSettings(builder ->
							builder.maxSize(conf.cores).minSize(conf.cores).maxConnecting(conf.cores).maxConnectionIdleTime(10, TimeUnit.MINUTES))
					.applyToClusterSettings(builder ->
							builder.hosts(Arrays.asList(new ServerAddress(conf.sourceHost, conf.sourcePort))))
					.build();
		}

		//----------------------------------------------------------------------------------------------
		// PREPARING THREAD POOLS
		//----------------------------------------------------------------------------------------------
		//System.out.println("Number of hardware threads: " + Runtime.getRuntime().availableProcessors());
		//System.out.println("Number of requested threads: " + conf.cores);


		//System.out.println("Counter threads: " + (Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors())));
		LOGGER.info("COUNTER THREADS=" + Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors()));
		LOGGER.info("COUNTER THREADS PRIORITY=" + (WANTED_THREAD_PRIORITY - 1));

		counterThreadPool = Executors.newVirtualThreadPerTaskExecutor(); /*Executors.newFixedThreadPool(Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors()), new ThreadFactory() {
			private final AtomicInteger threadNumber = new AtomicInteger(0);
			private final ThreadGroup group = new ThreadGroup("MongoDBMigration");

			@Override
			public Thread newThread(Runnable r) {
				final Thread t = new Thread(group, r,
						String.valueOf(threadNumber.getAndIncrement()),
						0);
				if (t.isDaemon())
					t.setDaemon(false);
				if (t.getPriority() != WANTED_THREAD_PRIORITY - 1)
					t.setPriority(WANTED_THREAD_PRIORITY - 1);
				return t;
			}
		}); */

//        System.out.println("Worker threads: "+(conf.useRSI ? 2* conf.cores/3 : conf.cores));
		//System.out.println("Worker threads: 8");

		LOGGER.info("WORKER THREADS=" + conf.cores);
		LOGGER.info("WORKER THREADS PRIORITY=" + WANTED_THREAD_PRIORITY);

		workerThreadPool = counterThreadPool; /*Executors.newVirtualThreadPerTaskExecutor();*/ /*Executors.newFixedThreadPool(conf.cores, //conf.useRSI ? 2* conf.cores/3 : conf.cores
				new ThreadFactory() {
					private final AtomicInteger threadNumber = new AtomicInteger(0);
					private final ThreadGroup group = new ThreadGroup("MongoDBMigration");

					@Override
					public Thread newThread(Runnable r) {
						final Thread t = new Thread(group, r,
								String.valueOf(threadNumber.getAndIncrement()),
								0);
						if (t.isDaemon())
							t.setDaemon(false);
						if (t.getPriority() != WANTED_THREAD_PRIORITY)
							t.setPriority(WANTED_THREAD_PRIORITY);
						return t;
					}
				});*/

		if (conf.sourceDump) {
			try {
				final PoolDataSource pds = initializeConnectionPool(false, conf.destinationConnectionString, conf.destinationUsername, conf.destinationPassword, conf.useRSI ? 3 : conf.cores);
				final PoolDataSource adminPDS = initializeConnectionPool(true, conf.destinationConnectionString, conf.destinationAdminUser, conf.destinationAdminPassword, 3);
				conf.initializeMaxParallelDegree(adminPDS);
				gui.setPDS(adminPDS);

				// Get destination database information
				try (Connection c = adminPDS.getConnection()) {
					try (Statement s = c.createStatement()) {
						try (ResultSet r = s.executeQuery("select version_full, count(*) from gv$instance group by version_full")) {
							if (r.next()) {
								final String oracleVersion = r.getString(1);
								int pos = oracleVersion.indexOf('.');
								pos = oracleVersion.indexOf('.', pos + 1);
								gui.setDestinationDatabaseVersion(oracleVersion.substring(0, pos));
								gui.setDestinationDatabaseInstances(r.getInt(2));
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
							}
						}
					}
				}
				catch (SQLException sqle) {
					sqle.printStackTrace();
				}

				if (conf.useMemoptimizeForWrite && !AUTONOMOUS_DATABASE) {
					throw new RuntimeException("Can't use memoptimize for write if target is not an autonomous database!");
				}

				gui.setsourceDatabaseVersion("(dump)");

				// get number of collections in this database
				MONGODB_COLLECTIONS = 0;
				// get number of indexes in this database
				MONGODB_INDEXES = 0;
				MongoDatabaseDump mongoDatabase = new MongoDatabaseDump(conf.sourceDumpFolder);
				for (String c : mongoDatabase.listCollectionsDump()) {
					MONGODB_COLLECTIONS++;
					MONGODB_INDEXES += mongoDatabase.getNumberOfIndexesForCollection(c);
				}
				gui.setNumberOfMongoDBCollections(MONGODB_COLLECTIONS);
				gui.setNumberOfMongoDBIndexes(MONGODB_INDEXES);
				gui.setNumberOfMongoDBJSONDocuments(0);
				gui.setTotalMongoDBSize(0);


				// Disabling Automatic ADB-S tasks if any
				OracleAutoTasks.disableIfNeeded(adminPDS);

				final PoolDataSource mediumPDS = MediumServiceManager.configure(adminPDS, pds, conf.destinationPassword);

				final Semaphore DB_SEMAPHORE = new Semaphore(conf.cores);

				for (String collectionName : mongoDatabase.listCollectionsDump()) {
					if (!conf.selectedCollections.isEmpty() && !conf.selectedCollections.contains(collectionName)) {
						//System.out.println("Collection " + collectionName + " will not be migrated.");
						continue;
					}

					final long startTimeCollection = System.currentTimeMillis();

					// disable is JSON constraint, remove indexes...
					final OracleCollectionInfo oracleCollectionInfo = OracleCollectionInfo.getCollectionInfoAndPrepareIt(pds, adminPDS, conf.destinationUsername.toUpperCase(), collectionName, conf.dropAlreadyExistingCollection, AUTONOMOUS_DATABASE, conf.useMemoptimizeForWrite);

					if (!oracleCollectionInfo.emptyDestinationCollection) {
						//System.out.println("Collection " + collectionName + " will not be migrated because destination is not empty!");
						continue;
					}

					gui.addNewDestinationDatabaseCollection(collectionName, null, mongoDatabase.getCollectionMetadata(collectionName));
/*
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
*/
					//System.out.println(collectionName + ": bucket size= " + bucketSize + ", iterations: " + iterations);

					final List<CollectionCluster> publishingCfs = new LinkedList<>();

					// scan the BSON data
					// - compute averageDocumentSize
					// - split file into 5,000,000 BSON packets collection (denoting file position start)

/*					long tempMin = minId;
					final long startClusterAnalysis = System.currentTimeMillis();

					for (long i = 0; i < iterations; i++, tempMin += bucketSize) {

						final long _max = i == iterations - 1 ? maxId + 1 : tempMin + bucketSize;

						//System.out.println(i + " From " + tempMin + " to " + _max);
						//System.out.println(i + "\t=> from " + Long.toHexString(tempMin) + " to " + Long.toHexString(_max));

						final CompletableFuture<CollectionCluster> publishingCf = new CompletableFuture<>();
						publishingCfs.add(publishingCf);
						counterThreadPool.execute(new CollectionClusteringAnalyzer(i, collectionName, publishingCf, tempMin, _max, mongoDatabase, gui, averageDocumentSize));
					}
*/
					final File bsonFile = mongoDatabase.getBSONFile(collectionName);

					long count = 0;
					position = previousPosition = 0;

					try (
							InputStream inputStream = bsonFile.getName().toLowerCase().endsWith(".gz") ?
									new GZIPInputStream(new FileInputStream(bsonFile), 128 * 1024 * 1024)
									: new BufferedInputStream(new FileInputStream(bsonFile), 128 * 1024 * 1024)
					) {
						long clusterStartPosition = 0;
						long clusterCount = 0;
						while (true) {
							try {
								//final byte[] data = readNextBSONRawData(inputStream);
								skipNextBSONRawData(inputStream);
								clusterCount++;

								// limit cluster size to 100,000 documents or 2 GB
								boolean sizeOverFlow=false;
								if((sizeOverFlow= ((position - clusterStartPosition) > 2048L*1024L*1024L)) || clusterCount == 100000) {
									if(sizeOverFlow) {
										clusterCount--;
										count += clusterCount;
										publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition,(int)(previousPosition-clusterStartPosition)));
										LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
										gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long)((double)(previousPosition-clusterStartPosition)/(double)clusterCount));
										clusterCount = 1;
										clusterStartPosition = previousPosition;
									} else {
										count += clusterCount;
										publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition,(int)(position-clusterStartPosition)));
										LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
										gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long)((double)(position-clusterStartPosition)/(double)clusterCount));
										clusterCount = 0;
										clusterStartPosition = position;
									}
								}

							} catch (EOFException eof) {
								break;
							}
						}

						if( clusterCount > 0 ) {
							final boolean sizeOverFlow= (position - clusterStartPosition) > 2048L*1024L*1024L;
							count += clusterCount;
							publishingCfs.add(new CollectionCluster(clusterCount, clusterStartPosition,(int)(position-clusterStartPosition)));
							LOGGER.info("- adding cluster of "+clusterCount+" JSON document(s).");
							gui.updateSourceDatabaseDocuments(clusterCount, clusterCount == 0 ? 0 : (long)((double)(position-clusterStartPosition)/(double)clusterCount));
						}
					}

					LOGGER.info("Collection "+collectionName+" has "+count+" JSON document(s).");

					final List<CompletableFuture<ConversionInformation>> publishingCfsConvert = new LinkedList<>();
					final List<CollectionCluster> mongoDBCollectionClusters = new ArrayList<>();

					long total = 0;
					int i = 0;
					final List<CollectionCluster> clusters = new ArrayList<>();

					if (conf.useRSI) {
						rsi = ReactiveStreamsIngestion
								.builder()
								.url("jdbc:oracle:thin:@" + conf.destinationConnectionString)
								.username(conf.destinationUsername)
								.password(conf.destinationPassword)
								.schema(conf.destinationUsername)
								.executor(rsiWorkerThreadPool)
								//.bufferInterval(Duration.ofMillis(1000L))
								.bufferRows(conf.RSIbufferRows /*49676730*/)
								.rowsPerBatch(conf.batchSize)
//                            .averageMessageSize(32*1024*1024)
								//.bufferInterval(Duration.ofSeconds(20))
//                            .bufferInterval(Duration.ofSeconds(1L))
								.table(collectionName)
								.columns(new String[]{"ID", /*"CREATED_ON", "LAST_MODIFIED",*/ "VERSION", conf.mongodbAPICompatible ? "DATA" : "JSON_DOCUMENT"})
								.useDirectPath()
								.useDirectPathNoLog()
								.useDirectPathParallel()
								.useDirectPathSkipIndexMaintenance()
								.useDirectPathSkipUnusableIndexes()
								.useDirectPathStorageInit(String.valueOf(8 * 1024 * 1024))
								.useDirectPathStorageNext(String.valueOf(8 * 1024 * 1024))
								.build();
					}

					for (CollectionCluster cc : publishingCfs) {
						clusters.add(cc);

						if (cc.count > 0) {
							total += cc.count;
							mongoDBCollectionClusters.add(cc);

							final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
							publishingCfsConvert.add(pCf);
/*							if (conf.useRSI) {
								workerThreadPool.execute(AUTONOMOUS_DATABASE ? new RSIBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, rsi, gui, conf.batchSize) :
										new RSIBSON2TextCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, rsi, gui, conf.batchSize));
							}
							else if (conf.useMemoptimizeForWrite) {
								workerThreadPool.execute(new MemoptimizeForWriteBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize));
							}
							else {
*/								workerThreadPool.execute(AUTONOMOUS_DATABASE ? new DirectDirectPathBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE) :
										new BSON2TextCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize));
/*							}
*/
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

					if (conf.useRSI) {
						rsi.close();
					}

					// TODO: manage indexes (build parallel using MEDIUM service changed configuration)
					oracleCollectionInfo.finish(mediumPDS, null, conf.maxSQLParallelDegree, gui);
					//gui.finishCollection();

				}

				gui.finishLastCollection();

				gui.stop();

				// Disabling Automatic ADB-S tasks if any
				OracleAutoTasks.enableIfNeeded(adminPDS);

				MediumServiceManager.restore(adminPDS);

				gui.flushTerminal();
			}
			catch(Throwable t){
				t.printStackTrace();
			}
			finally{
				if (counterThreadPool != null) {
					counterThreadPool.shutdown();
				}
				if (workerThreadPool != null) {
					workerThreadPool.shutdown();
				}
			}
		}
		else {
			try (MongoClient mongoClient = MongoClients.create(settings)) {
				final PoolDataSource pds = initializeConnectionPool(false, conf.destinationConnectionString, conf.destinationUsername, conf.destinationPassword, conf.useRSI ? 3 : conf.cores);
				final PoolDataSource adminPDS = initializeConnectionPool(true, conf.destinationConnectionString, conf.destinationAdminUser, conf.destinationAdminPassword, 3);
				conf.initializeMaxParallelDegree(adminPDS);
				gui.setPDS(adminPDS);

				// Get destination database information
				try (Connection c = adminPDS.getConnection()) {
					try (Statement s = c.createStatement()) {
						try (ResultSet r = s.executeQuery("select version_full, count(*) from gv$instance group by version_full")) {
							if (r.next()) {
								final String oracleVersion = r.getString(1);
								int pos = oracleVersion.indexOf('.');
								pos = oracleVersion.indexOf('.', pos + 1);
								gui.setDestinationDatabaseVersion(oracleVersion.substring(0, pos));
								gui.setDestinationDatabaseInstances(r.getInt(2));
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
							}
						}
					}
				}
				catch (SQLException sqle) {
					sqle.printStackTrace();
				}

				if (conf.useMemoptimizeForWrite && !AUTONOMOUS_DATABASE) {
					throw new RuntimeException("Can't use memoptimize for write if target is not an autonomous database!");
				}

				MongoDatabase mongoDatabase = mongoClient.getDatabase(conf.sourceDatabase);
				final Document result = mongoDatabase.runCommand(new Document("buildInfo", 1));
				gui.setsourceDatabaseVersion((String) result.get("version"));

				// get number of collections in this database
				MONGODB_COLLECTIONS = 0;
				// get number of indexes in this database
				MONGODB_INDEXES = 0;
				for (Document d : mongoDatabase.listCollections()) {
					MONGODB_COLLECTIONS++;

					final MongoCollection<Document> collection = mongoDatabase.getCollection(d.getString("name"));
					for (Document i : collection.listIndexes()) {
						MONGODB_INDEXES++;
					}
				}
				gui.setNumberOfMongoDBCollections(MONGODB_COLLECTIONS);
				gui.setNumberOfMongoDBIndexes(MONGODB_INDEXES);
				gui.setNumberOfMongoDBJSONDocuments(0);
				gui.setTotalMongoDBSize(0);


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

					final MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);

					// disable is JSON constraint, remove indexes...
					final OracleCollectionInfo oracleCollectionInfo = OracleCollectionInfo.getCollectionInfoAndPrepareIt(pds, adminPDS, conf.destinationUsername.toUpperCase(), collectionName, conf.dropAlreadyExistingCollection, AUTONOMOUS_DATABASE, conf.useMemoptimizeForWrite);

					if (!oracleCollectionInfo.emptyDestinationCollection) {
						//System.out.println("Collection " + collectionName + " will not be migrated because destination is not empty!");
						continue;
					}

					gui.addNewDestinationDatabaseCollection(collectionName, mongoCollection, null);

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

					if (conf.useRSI) {
						rsi = ReactiveStreamsIngestion
								.builder()
								.url("jdbc:oracle:thin:@" + conf.destinationConnectionString)
								.username(conf.destinationUsername)
								.password(conf.destinationPassword)
								.schema(conf.destinationUsername)
								.executor(rsiWorkerThreadPool)
								//.bufferInterval(Duration.ofMillis(1000L))
								.bufferRows(conf.RSIbufferRows /*49676730*/)
								.rowsPerBatch(conf.batchSize)
//                            .averageMessageSize(32*1024*1024)
								//.bufferInterval(Duration.ofSeconds(20))
//                            .bufferInterval(Duration.ofSeconds(1L))
								.table(collectionName)
								.columns(new String[]{"ID", /*"CREATED_ON", "LAST_MODIFIED",*/ "VERSION", conf.mongodbAPICompatible ? "DATA" : "JSON_DOCUMENT"})
								.useDirectPath()
								.useDirectPathNoLog()
								.useDirectPathParallel()
								.useDirectPathSkipIndexMaintenance()
								.useDirectPathSkipUnusableIndexes()
								.useDirectPathStorageInit(String.valueOf(8 * 1024 * 1024))
								.useDirectPathStorageNext(String.valueOf(8 * 1024 * 1024))
								.build();
					}

					for (CompletableFuture<CollectionCluster> publishingCf : publishingCfs) {
						CollectionCluster cc = publishingCf.join();
						clusters.add(cc);

						if (cc.count > 0) {
							total += cc.count;
							mongoDBCollectionClusters.add(cc);

							final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
							publishingCfsConvert.add(pCf);
							if (conf.useRSI) {
								workerThreadPool.execute(AUTONOMOUS_DATABASE ? new RSIBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, rsi, gui, conf.batchSize) :
										new RSIBSON2TextCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, rsi, gui, conf.batchSize));
							}
							else if (conf.useMemoptimizeForWrite) {
								workerThreadPool.execute(new MemoptimizeForWriteBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize));
							}
							else {
								workerThreadPool.execute(AUTONOMOUS_DATABASE ? new DirectDirectPathBSON2OSONCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize, DB_SEMAPHORE) :
										new BSON2TextCollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize));
							}

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

					if (conf.useRSI) {
						rsi.close();
					}

					// TODO: manage indexes (build parallel using MEDIUM service changed configuration)
					oracleCollectionInfo.finish(mediumPDS, mongoCollection, conf.maxSQLParallelDegree, gui);
					//gui.finishCollection();

				}

				gui.finishLastCollection();

				gui.stop();

				// Disabling Automatic ADB-S tasks if any
				OracleAutoTasks.enableIfNeeded(adminPDS);

				MediumServiceManager.restore(adminPDS);

				gui.flushTerminal();
			}
			catch(Throwable t){
				t.printStackTrace();
			}
			finally{
				if (counterThreadPool != null) {
					counterThreadPool.shutdown();
				}
				if (workerThreadPool != null) {
					workerThreadPool.shutdown();
				}
			}
		}
	}

	private static PoolDataSource initializeConnectionPool(boolean admin, String ajdConnectionService, String user, String password, int cores) throws SQLException, IOException {
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");

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
