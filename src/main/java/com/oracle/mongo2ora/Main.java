package com.oracle.mongo2ora;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.Filters;
import com.oracle.mongo2ora.asciigui.ASCIIGUI;
import com.oracle.mongo2ora.migration.Configuration;
import com.oracle.mongo2ora.migration.ConversionInformation;
import com.oracle.mongo2ora.migration.mongodb.CollectionCluster;
import com.oracle.mongo2ora.migration.oracle.MediumServiceManager;
import com.oracle.mongo2ora.migration.oracle.OracleAutoTasks;
import com.oracle.mongo2ora.migration.oracle.OracleCollectionInfo;
import com.oracle.mongo2ora.migration.oracle.ThroughputDisplayerTask;
import com.oracle.mongo2ora.util.Kernel32;
import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.Native;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalSize;
import net.rubygrapefruit.platform.terminal.Terminals;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.MyBSONDecoder;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Projections.include;
import static com.oracle.mongo2ora.util.XYTerminalOutput.*;
import static java.util.stream.Collectors.toList;

public class Main {
	public static final String VERSION = "1.1.0";

	public static boolean ENABLE_COLORS = true;

	public static final int WANTED_THREAD_PRIORITY = Thread.NORM_PRIORITY + 1;

	private static Terminals TERMINALS;
	public static XYTerminalOutput TERM;

	static final Document useIdIndexHint = new Document("_id", 1);

	// TODO: dychotomic search
	static class CollectionClusteringAnalyzer implements Runnable {
		private final long id;
		private final CompletableFuture<CollectionCluster> publishingCf;
		private final long _min;
		private final long _max;
		private final MongoDatabase database;
		private final long averageDocSize;
		private final String collectionName;
		private final ASCIIGUI gui;

		public CollectionClusteringAnalyzer(long id, String collectionName, CompletableFuture<CollectionCluster> publishingCf, long _min, long _max, MongoDatabase database, ASCIIGUI gui, long averageDocSize) {
			this.id = id;
			this.collectionName = collectionName;
			this.publishingCf = publishingCf;
			this._min = _min;
			this._max = _max;
			this.database = database;
			this.gui = gui;
			this.averageDocSize = averageDocSize;
		}

		@Override
		public void run() {
			long startTime = System.currentTimeMillis();
			long docNumber = 0;

			final ObjectId minId = new ObjectId(Long.toHexString(_min) + "0000000000000000");
			final ObjectId maxId = new ObjectId(Long.toHexString(_max) + "0000000000000000");

			try {
				MongoCollection<Document> collection = database.getCollection(collectionName);

				docNumber = collection.countDocuments(Filters.and(
								Filters.gte("_id", minId),
								Filters.lt("_id", maxId))
						, new CountOptions().hint(useIdIndexHint));

				if (docNumber > 0) {
					//System.out.println("Thread " + id + ": from " + minId + " to " + maxId + " has " + docNumber + " docs in " + (System.currentTimeMillis() - startTime) + "ms");
					gui.updateSourceDatabaseDocuments(docNumber, averageDocSize );
					//updateMongoDBDocs(database.getName(), MONGODB_COLLECTIONS, MONGODB_INDEXES, docNumber, averageDocSize);
				}
				else {
					//System.out.println("Thread " + id + ": nothing found in " + (System.currentTimeMillis() - startTime) + "ms");
				}
			}
			finally {
				publishingCf.complete(new CollectionCluster(docNumber, minId, maxId));
			}
		}
	}

	static ExecutorService workerThreadPool;
	static ExecutorService counterThreadPool;

	static int MONGODB_COLLECTIONS = 0;
	static int MONGODB_INDEXES = 0;

	public static ASCIIGUI gui;

	public static void main(final String[] args) {
		// For Autonomous Database CMAN load balancing
		Security.setProperty("networkaddress.cache.ttl", "0");

		Locale.setDefault(Locale.US);

		// will run only on Windows OS
		Kernel32.init();

		TERMINALS = Native.get(Terminals.class).withAnsiOutput();

		if (!TERMINALS.isTerminal(Terminals.Output.Stdout)) {
			return;
		}

		TERM = new XYTerminalOutput(TERMINALS.getTerminal(Terminals.Output.Stdout));
		System.setOut(new PrintStream(TERM.getOutputStream(), true));

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

		// Connect to MongoDB database
		final MongoCredential credential = MongoCredential.createCredential(conf.sourceUsername, conf.sourceDatabase, conf.sourcePassword.toCharArray());

		final MongoClientSettings settings = conf.sourceUsername == null || conf.sourceUsername.isEmpty() ?
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


		//----------------------------------------------------------------------------------------------
		// PREPARING THREAD POOLS
		//----------------------------------------------------------------------------------------------
		//System.out.println("Number of hardware threads: " + Runtime.getRuntime().availableProcessors());
		//System.out.println("Number of requested threads: " + conf.cores);


		//System.out.println("Counter threads: " + (Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors())));
		counterThreadPool = Executors.newFixedThreadPool(Math.min(conf.cores * 2, Runtime.getRuntime().availableProcessors()), new ThreadFactory() {
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
		});

//        System.out.println("Worker threads: "+(conf.useRSI ? 2* conf.cores/3 : conf.cores));
		//System.out.println("Worker threads: 8");
		workerThreadPool = Executors.newFixedThreadPool(8 /*conf.useRSI ? 2* conf.cores/3 : conf.cores*/,
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

		try (MongoClient mongoClient = MongoClients.create(settings)) {
			//System.out.println("Initializing Oracle database connection pools...");
			final PoolDataSource pds = initializeConnectionPool(false, conf.destinationConnectionString, conf.destinationUsername, conf.destinationPassword, conf.cores);
			final PoolDataSource adminPDS = initializeConnectionPool(true, conf.destinationConnectionString, conf.destinationAdminUser, conf.destinationAdminPassword, 3);
			conf.initializeMaxParallelDegree(adminPDS);
			gui.setPDS(adminPDS);
			//new Timer("Throughput timer", true).scheduleAtFixedRate(new ThroughputDisplayerTask(adminPDS, TERM), 0, 1000);

			try (Connection c = adminPDS.getConnection()) {
				try (Statement s = c.createStatement()) {
					try (ResultSet r = s.executeQuery("select version_full, count(*) from gv$instance group by version_full")) {
						if (r.next()) {
							final String oracleVersion = r.getString(1);
							int pos = oracleVersion.indexOf('.');
							pos = oracleVersion.indexOf('.', pos + 1);
							gui.setDestinationDatabaseVersion(oracleVersion.substring(0, pos));
							//setInstancesNumber(r.getInt(2));
						}
					}
				}
			}
			catch (SQLException sqle) {
				sqle.printStackTrace();
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

			for (Document collectionDescription : mongoDatabase.listCollections()) {
				final String collectionName = collectionDescription.getString("name");

				if (!conf.selectedCollections.isEmpty() && !conf.selectedCollections.contains(collectionName)) {
					//System.out.println("Collection " + collectionName + " will not be migrated.");
					continue;
				}

				final long startTimeCollection = System.currentTimeMillis();

				final MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);

				// disable is JSON constraint, remove indexes...
				final OracleCollectionInfo oracleCollectionInfo = OracleCollectionInfo.getCollectionInfoAndPrepareIt(pds, adminPDS, conf.destinationUsername.toUpperCase(), collectionName, conf.dropAlreadyExistingCollection, mongoCollection);

				if (!oracleCollectionInfo.emptyDestinationCollection) {
					//System.out.println("Collection " + collectionName + " will not be migrated because destination is not empty!");
					continue;
				}

				gui.addNewDestinationDatabaseCollection(collectionName);

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

				final long minId = Long.parseLong(min.get("_id").toString().substring(0, 8), 16);
				final long maxId = Long.parseLong(max.get("_id").toString().substring(0, 8), 16);

				//System.out.println("min 8 first chars _id: " + minId);
				//System.out.println("max 8 first chars _id: " + maxId);


				long bucketSize = 8L;

				long iterations = ((maxId - minId) / bucketSize) + 1;

				// We don't want to start 100000+ threads!
				while(iterations > 100000) {
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

				for (CompletableFuture<CollectionCluster> publishingCf : publishingCfs) {
					CollectionCluster cc = publishingCf.join();
					clusters.add(cc);

					if (cc.count > 0) {
						total += cc.count;
						mongoDBCollectionClusters.add(cc);

						final CompletableFuture<ConversionInformation> pCf = new CompletableFuture<>();
						publishingCfsConvert.add(pCf);
						workerThreadPool.execute(new CollectionConverter(i % 256, collectionName, cc, pCf, mongoDatabase, pds, gui, conf.batchSize));

						i++;
					}
				}

				//System.out.println("Docs: " + total + " for " + mongoDBCollectionClusters.size() + " cluster(s)");
				//println(Console.Style.ANSI_BLUE + "Collection clustering analysis duration: " + getDurationSince(startClusterAnalysis));

				clusters.clear();

				final List<ConversionInformation> informations = publishingCfsConvert.stream().map(CompletableFuture::join).collect(toList());

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
		pds.setTimeoutCheckInterval(120);
		pds.setInactiveConnectionTimeout(120);
		pds.setValidateConnectionOnBorrow(!admin);
		pds.setMaxStatements(20);
		pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
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

        /*
        try {
            try (InputStream in = new FileInputStream("/home/opc/jsonloader/jdbc.properties")) {
                Properties p = new Properties();
                p.load(in);

                for (Object k : p.keySet()) {
                    String key = (String) k;

                    System.out.println("Adding property " + key + "=" + p.getProperty(key));
                    pds.setConnectionProperty(key, p.getProperty(key));
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        */

		return pds;
	}

	private static long maxMongoDBDocs;

	public static synchronized void updateMongoDBDocs(String dbName, int collections, int indexes, long totalMongoDBDocs, long avgJSONSize) {
		maxMongoDBDocs += totalMongoDBDocs;
		TERM.reset().moveTo(dbName.length() + 3, 1).write(": ").bold().bright().write(String.format("%,d", collections)).reset().write(" collection(s), ");
		TERM.bold().bright().write(String.format("%,d", maxMongoDBDocs)).reset().write(" JSON docs, ");
		TERM.bold().bright().write(String.format("%,d", indexes)).reset().write(" index(es), ");

		double avgSize = (double) (maxMongoDBDocs * avgJSONSize) / 1024d / 1024d / 1024d;
		if (avgSize > 1024d) {
			avgSize /= 1024d;
			TERM.bold().bright().write(String.format("%.1f", avgSize)).reset().write(" TB");
		}
		else {
			TERM.bold().bright().write(String.format("%.1f", avgSize)).reset().write(" GB");
		}

//		collectionsPanel.updateMongoDBInfo(totalMongoDBDocs, avgJSONSize);
	}

	static class CollectionConverter implements Runnable {
		private final CollectionCluster work;
		private final CompletableFuture<ConversionInformation> publishingCf;
		private final PoolDataSource pds;
		private final MongoDatabase database;
		private final int partitionId;
				private final ASCIIGUI gui;
		private final int batchSize;
		private final String collectionName;

		public CollectionConverter(int partitionId, String collectionName, CollectionCluster work, CompletableFuture<ConversionInformation> publishingCf, MongoDatabase database, PoolDataSource pds, ASCIIGUI gui, int batchSize) {
			this.partitionId = partitionId;
			this.collectionName = collectionName;
			this.work = work;
			this.publishingCf = publishingCf;
			this.database = database;
			this.pds = pds;
			this.gui = gui;
			this.batchSize = batchSize;
		}

		@Override
		public void run() {
			long bsonLength = 0;
			long osonLength = 0;
			long count = 0;
			final int threadId = Integer.parseInt(Thread.currentThread().getName());

			//System.out.println("Thread " + threadId + " working on " + work.count + " docs");

			try {
				MongoCollection<RawBsonDocument> collection = database.getCollection(collectionName, RawBsonDocument.class);
				try (Connection c = pds.getConnection()) {
					c.setAutoCommit(false);

                    /*
                    try (Statement s = c.createStatement()) {
                        try (ResultSet r = s.executeQuery("select sys_context('USERENV','INSTANCE') from dual")) {
                            if (r.next()) {
                                System.out.println("Starting job with thread id: " + Thread.currentThread().getName() + ", partition id: " + partitionId + " on instance id: " + r.getInt(1));
                            }
                        }
                    }
                    */

					final OracleConnection realConnection = (OracleConnection) c;

					try (MongoCursor<RawBsonDocument> cursor = collection.find(Filters.and(
									Filters.gte("_id", work.minId),
									Filters.lt("_id", work.maxId)
							)
					).hint(useIdIndexHint).batchSize(2048 /*batchSize*2*/).cursor()) {
						long start = System.currentTimeMillis();

						final EnumSet<OracleConnection.CommitOption> commitOptions = EnumSet.of(
								oracle.jdbc.internal.OracleConnection.CommitOption.WRITEBATCH,
								oracle.jdbc.internal.OracleConnection.CommitOption.NOWAIT);

						final Properties directPathLoadProperties = new Properties();


						directPathLoadProperties.put("DPPDEF_IN_NOLOG", "true");
						directPathLoadProperties.put("DPPDEF_IN_PARALLEL", "true");
						directPathLoadProperties.put("DPPDEF_IN_SKIP_UNUSABLE_INDEX", "true");
						directPathLoadProperties.put("DPPDEF_IN_SKIP_INDEX_MAINT", "true");
						directPathLoadProperties.put("DPPDEF_IN_STORAGE_INIT", String.valueOf(8 * 1024 * 1024));
						directPathLoadProperties.put("DPPDEF_IN_STORAGE_NEXT", String.valueOf(8 * 1024 * 1024));


						try (PreparedStatement p = ((OracleConnection) c).prepareDirectPath(pds.getUser().toUpperCase(), collectionName, new String[]{"ID", "VERSION", "JSON_DOCUMENT"},/* String.format("p%d", partitionId),*/ directPathLoadProperties)) {
						//try (PreparedStatement p = c.prepareStatement("insert /*+ append */ into " + collectionName + " (ID, VERSION, JSON_DOCUMENT) values (?,?,?)")) {
                            /*final CharacterSet cs = CharacterSet.make(CharacterSet.AL32UTF8_CHARSET);
                            final CHAR version = new CHAR("1", cs);

                            p.setObject(3, version, oracle.jdbc.OracleTypes.CHAR); */
							p.setString(2, "1");

							int batchSizeCounter = 0;

							//final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
							//final MyStringReader msr = new MyStringReader("");

							final MyBSONDecoder decoder = new MyBSONDecoder(true);

							while (cursor.hasNext()) {
								//out.reset();
								final RawBsonDocument doc = cursor.next();

								// -500 MB/sec
								decoder.convertBSONToOSON(doc);
								bsonLength += decoder.getBsonLength();

//                                OracleJsonGenerator ogen = factory.createJsonBinaryGenerator(out);
								//JsonGenerator gen = ogen.wrap(JsonGenerator.class);
//                                msr.reset("{\"test\":1}");
								//msr.reset(doc.toJson(jsonWriterSettings));
//                                ogen.writeParser(factory.createJsonTextParser(msr));
//                                ogen.close();

//                                p.setString(1, doc.get("_id").toString());
//                                p.setBytes(2, out.toByteArray());
								byte[] osonData;
								p.setBytes(3, osonData = decoder.getOSONData());
//								p.setObject(3,osonData = decoder.getOSONData(), OracleTypes.JSON);
//								p.setString(3, doc.toJson());
//								osonData = doc.toJson().getBytes(StandardCharsets.UTF_8);


								p.setString(1, decoder.getOid());
								//final CHAR oid = new CHAR(decoder.getOid(), cs);

								//p.setObject(1, oid, oracle.jdbc.OracleTypes.CHAR);


								osonLength += osonData.length;

								p.addBatch();

								batchSizeCounter++;

								if (batchSizeCounter >= batchSize) {
									count += batchSizeCounter;
									p.executeLargeBatch();
									batchSizeCounter = 0;
								}
							}

							if (batchSizeCounter > 0) {
								count += batchSizeCounter;
								p.executeLargeBatch();
							}

							realConnection.commit(commitOptions);

							final long duration = System.currentTimeMillis() - start;
							gui.updateDestinationDatabaseDocuments(count,osonLength);
							//System.out.println("Thread " + threadId + " got " + count + " docs in " + duration + "ms => " + ((double) count / (double) duration * 1000.0d) + " Docs/s (BSON: " + bsonLength + ", OSON: " + osonLength + ")");
						}
					}
				}
			}
			catch (SQLException sqle) {
				sqle.printStackTrace();
			}
			finally {
				//System.out.println("Completed conversion task with: " + bsonLength + ", " + osonLength + "," + count);
				publishingCf.complete(new ConversionInformation(bsonLength, osonLength, count));
			}
		}
	}
}
