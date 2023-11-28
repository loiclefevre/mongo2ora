package com.oracle.mongo2ora.migration;

import com.oracle.mongo2ora.Main;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Configuration {
	public boolean useRSI;
	public boolean useMemoptimizeForWrite;
	public boolean dropAlreadyExistingCollection;
	public String source;
	public String destination;
	public int batchSize = 4096;
	public int cores = Runtime.getRuntime().availableProcessors();
	public boolean awrReport;
	public long startSnapID = -1;
	public long endSnapID = -1;
	public String awrDescription = "";

	public String sourceUsername;
	public String sourcePassword;
	public String sourceDatabase;
	public String sourceHost;
	public int sourcePort;

	public String destinationAdminUser;
	public String destinationAdminPassword;
	public String destinationUsername;
	public String destinationPassword;
	public String destinationConnectionString;
	public String destinationDatabase;

	public final List<String> selectedCollections = new ArrayList<>();
	public int maxSQLParallelDegree;

	public int RSIThreads = Math.max(1, (int) (Runtime.getRuntime().availableProcessors() / 3));

	public int RSIbufferRows = 64 * 1024;

	public boolean sourceDump;

	public String sourceDumpFolder;
	public boolean mongodbAPICompatible;
	public boolean forceOSON;
	public boolean skipSecondaryIndexes;
	public boolean buildSecondaryIndexes;

	public long samples = -1;

	public boolean allowDuplicateKeys;

	public final  Properties collectionsProperties = new Properties();

	public long dumpBufferSize = 128;

	public static Configuration prepareConfiguration(String[] args) {
		Configuration conf = new Configuration();

		for (int i = 0; i < args.length; i++) {
			final String arg = args[i];
			switch (arg.toLowerCase()) {
				case "--allow-dup-keys":
					conf.allowDuplicateKeys = true;
					break;
				case "--samples":
					if (i + 1 < args.length) {
						conf.samples = Long.parseLong(args[++i]);
						if(conf.samples <= 0) {
							displayUsage("Expected valid samples parameter: --samples <strictly positive number of documents>");
						}
					}
					else {
						displayUsage("Expected valid samples parameter: --samples <strictly positive number of documents>");
					}
					break;

				case "--mongodbapi":
				case "--mongodb-api":
					conf.mongodbAPICompatible = true;
					break;

				case "--oson":
					conf.forceOSON = true;
					break;

				case "--build-secondary-indexes":
					conf.buildSecondaryIndexes = true;
					break;

				case "--skip-secondary-indexes":
					conf.skipSecondaryIndexes = true;
					break;

				case "-s":
					if (i + 1 < args.length) {
						conf.source = args[++i];
					}
					else {
						displayUsage("Expected valid source parameter: -s <source>");
					}
					break;

				case "--dump-buffer-size":
					if (i + 1 < args.length) {
						conf.dumpBufferSize = Math.max(16,Long.parseLong(args[++i]));
					}
					else {
						displayUsage("Expected valid --dump-buffer-size parameter: --dump-buffer-size <Mega Bytes of RAM to buffer mongodumps load (minimum 16)>");
					}
					break;

				case "-d":
					if (i + 1 < args.length) {
						conf.destination = args[++i];
					}
					else {
						displayUsage("Expected valid destination parameter: -d <destination>");
					}
					break;

				case "-da":
					if (i + 1 < args.length) {
						conf.destinationAdminUser = args[++i];
					}
					else {
						displayUsage("Expected valid destination admin username parameter: -da <destination admin username>");
					}
					break;

				case "-dp":
					if (i + 1 < args.length) {
						conf.destinationAdminPassword = args[++i];
					}
					else {
						displayUsage("Expected valid destination admin password parameter: -dp <destination admin password>");
					}
					break;

				case "-b":
					if (i + 1 < args.length) {
						conf.batchSize = Integer.parseInt(args[++i]);
						if (conf.batchSize <= 0) {
							displayUsage("Expected valid batch size parameter: -b <strictly positive number>");
						}
					}
					else {
						displayUsage("Expected valid batch size parameter: -b <number>");
					}
					break;

				case "-p":
					if (i + 1 < args.length) {
						conf.cores = Integer.parseInt(args[++i]);
						if (conf.cores <= 0) {
							displayUsage("Expected valid parallel threads parameter: -p <strictly positive number>");
						}
					}
					else {
						displayUsage("Expected valid parallel threads parameter: -p <number>");
					}
					break;

				case "-a":
					conf.awrReport = true;

					if (i + 1 < args.length) {
						if (!args[i + 1].startsWith("-")) {
							conf.awrDescription = args[++i];
						}
					}
					break;

				case "-c":
					if (i + 1 < args.length) {
						Collections.addAll(conf.selectedCollections, args[++i].split(","));
					}
					else {
						displayUsage("Expected valid list of collection(s) to migrate parameter: -c <comma-separated list of collection name(s)>");
					}
					break;

				case "-r":
					conf.useRSI = true;
					break;

				case "-rt":
					if (i + 1 < args.length) {
						conf.RSIThreads = Integer.parseInt(args[++i]);
						if (conf.RSIThreads <= 0) {
							displayUsage("Expected valid RSI parallel threads parameter: -rt <strictly positive number>");
						}
					}
					else {
						displayUsage("Expected valid RSI parallel threads parameter: -rt <strictly positive number>");
					}
					break;

				case "-rbr":
					if (i + 1 < args.length) {
						conf.RSIbufferRows = Integer.parseInt(args[++i]);
						if (conf.RSIbufferRows < 0) {
							displayUsage("Expected valid RSI buffer rows parameter: -rbr <positive number, even 0 for auto-configuration>");
						}
					}
					else {
						displayUsage("Expected valid RSI buffer rows parameter: -rbr <positive number, even 0 for auto-configuration>");
					}
					break;

				case "-m":
					conf.useMemoptimizeForWrite = true;
					break;

				case "--drop":
					conf.dropAlreadyExistingCollection = true;
					break;
			}
		}

		conf.parseSource();
		conf.parseDestination();

		return conf;
	}

	public Configuration() {
		try {
			collectionsProperties.load(new FileInputStream("mongo2ora.properties"));
		}
		catch (IOException ignored) {
		}
	}

	private void parseDestination() {
		if (destination == null || destination.isEmpty()) {
			displayUsage("missing destination");
		}

		if (destination.contains("@")) {
			final String start = destination.substring(0, destination.indexOf("@"));

			if (start.contains("/")) {
				destinationUsername = start.substring(0, start.indexOf("/"));
				destinationPassword = start.substring(start.indexOf("/") + 1);
			}

			destinationConnectionString = destination.substring(destination.indexOf("@") + 1);
		}
	}

	private void parseSource() {
		if (source == null || source.isEmpty()) {
			displayUsage("missing source");
		}

		String temp = source;

		if (temp.startsWith("mongodb://")) {
			sourceDump = false;
			temp = temp.substring("mongodb://".length());

			if (temp.indexOf("@") != -1) {
				final String start = temp.substring(0, temp.indexOf("@"));

				if (start.indexOf(":") != -1) {
					sourceUsername = start.substring(0, start.indexOf(":"));
					sourcePassword = start.substring(start.indexOf(":") + 1);
				}

				final String end = temp.substring(temp.indexOf("@") + 1);

				if (end.indexOf(":") != -1) {
					sourceHost = end.substring(0, end.indexOf(":"));
					final String portDB = end.substring(end.indexOf(":") + 1);

					if (portDB.indexOf("/") != -1) {
						sourcePort = Integer.parseInt(portDB.substring(0, portDB.indexOf("/")));
						sourceDatabase = portDB.substring(portDB.indexOf("/") + 1);
					}
				}
			}
			else {
				sourceUsername = "";
				sourcePassword = "";

				if (temp.indexOf(":") != -1) {
					sourceHost = temp.substring(0, temp.indexOf(":"));
					final String portDB = temp.substring(temp.indexOf(":") + 1);

					if (portDB.indexOf("/") != -1) {
						sourcePort = Integer.parseInt(portDB.substring(0, portDB.indexOf("/")));
						sourceDatabase = portDB.substring(portDB.indexOf("/") + 1);
					}
				}
			}
		} else if (temp.startsWith("mongodump://")) {
			sourceDump = true;
			sourceDumpFolder = temp.substring("mongodump://".length());

			final File dumpFolder = new File(sourceDumpFolder);

			if(!dumpFolder.exists()) {
				displayUsage("mongodump folder \""+sourceDumpFolder+"\" doesn't exist!");
			}
			if(!dumpFolder.isDirectory()) {
				displayUsage("mongodump folder \""+sourceDumpFolder+"\" is not a folder!");
			}
			sourceDatabase = dumpFolder.getName();
		}
	}

	public static void displayUsage(String msg) {
		if (msg != null && !msg.isEmpty()) {
			Main.TERM.reset().bold().foreground(TerminalOutput.Color.Red).write(msg).newline().reset();
		}

		Main.TERM.write("Usage: mongo2ora -s <source> -d <destination> -da <destination admin username> -dp <destination admin password> [options...]").newline().newline()
				.write("where source can be:").newline()
				.write("- a valid MongoDB connection string optionally enclosed between quotes or double quotes,").newline()
				.write("- a valid MongoDB Dump folder connection string (example: 'mongodump:///u01/data/dump' or \"mogodump://C:/temp folder/dump\") optionally enclosed between quotes or double quotes,").newline()
				.write("and where destination can be a valid Oracle database connection string optionally enclosed between quotes or double quotes").newline().newline()
				.write("Options:").newline()
				.write("-p <number>: parallel threads to use (default number of vCPUs)").newline()
				.write("-b <batch size>: size of batch (default: 4096 JSON documents)").newline()
				//.write("-a [description]: generate AWR report for Oracle database with optional description (requires Diagnostic Pack for on-premises)").newline()
				.write("-c <comma-separated list of collection name(s)>: migrate only the selected collection(s)").newline()
				.write("--drop: drop existing collection(s) from the destination database (before recreating it if needed)").newline()
				.write("--skip-secondary-indexes: don't create secondary indexes (only for mongodumps)").newline()
				.write("--build-secondary-indexes: don't load data, only create secondary indexes (only for mongodumps)").newline()
				.write("--mongodb-api: makes collection(s) compatible to work with the Oracle Database API for MongoDB").newline()
				.write("--samples <number>: loads only number of JSON documents into the collection(s) (only for mongodumps)").newline()
				.write("--dump-buffer-size <number>: Mega Bytes of RAM to buffer mongodumps (default: 128 MB used per thread)").newline()
		;

		System.exit(1);
	}

	@Override
	public String toString() {
		return "Session configuration";/* Console.Style.ANSI_GREEN + "Session configuration:\n" + Console.Style.ANSI_RESET +
				"- source: " + source + "\n" +
				"\t. username: " + sourceUsername + "\n" +
				"\t. password: " + sourcePassword + "\n" +
				"\t. database: " + sourceDatabase + "\n" +
				"\t. host: " + sourceHost + "\n" +
				"\t. port: " + sourcePort + "\n" +
				"- destination: " + destination + "\n" +
				"\t. username: " + destinationUsername + "\n" +
				"\t. password: " + destinationPassword + "\n" +
				"\t. admin username: " + destinationAdminUser + "\n" +
				"\t. admin password: " + destinationAdminPassword + "\n" +
				"- parallel: " + cores + "\n" +
				"- batch size: " + batchSize + "\n" +
				"- AWR report generation: " + (awrReport ? "Yes" + (awrDescription.isEmpty() ? "" : " with description: " + awrDescription) : "No") + "\n" +
				(selectedCollections.isEmpty() ? "" : "- selected collection(s): " + String.join(", ", selectedCollections))
				;*/
	}

	public void initializeMaxParallelDegree(PoolDataSource adminPDS) throws SQLException {
		try (Connection c = adminPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				maxSQLParallelDegree = -1;
				try (ResultSet r = s.executeQuery("select sum(value), count(*) from gv$parameter where name = 'cpu_count'")) {
					if (r.next()) {
						maxSQLParallelDegree = r.getInt(1);
						//System.out.println("Max parallelism for SQL queries: " + maxSQLParallelDegree);
					}
				}
			}
		}
	}

	public void println() {
		Main.TERM.reset().foreground(TerminalOutput.Color.Green).write("Session configuration:").newline().reset()
				.write("- source: " + source).newline()
				.write("\t. username: " + sourceUsername).newline()
				.write("\t. password: " + sourcePassword).newline()
				.write("\t. database: " + sourceDatabase).newline()
				.write("\t. host: " + sourceHost).newline()
				.write("\t. port: " + sourcePort).newline()
				.write("- destination: " + destination).newline()
				.write("\t. username: " + destinationUsername).newline()
				.write("\t. password: " + destinationPassword).newline()
				.write("\t. admin username: " + destinationAdminUser).newline()
				.write("\t. admin password: " + destinationAdminPassword).newline()
				.write("- parallel: " + cores).newline()
				.write("- batch size: " + batchSize).newline()
				.write("- AWR report generation: " + (awrReport ? "Yes" + (awrDescription.isEmpty() ? "" : " with description: " + awrDescription) : "No")).newline()
				.write(selectedCollections.isEmpty() ? "" : "- selected collection(s): " + String.join(", ", selectedCollections)).newline();
	}
}
