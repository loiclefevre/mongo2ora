package com.oracle.mongo2ora.asciigui;

import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static com.oracle.mongo2ora.util.XYTerminalOutput.BrightRed;

public class ASCIIGUI extends TimerTask {
	private final String title;
	private String sourceDatabase;
	private String mongoDBVersion = "";
	private String destinationDatabaseName;
	private String oracleVersion = "";
	private final boolean WINDOWS;
	private long numberOfMongoDBCollections = -1;
	private long numberOfMongoDBJSONDocuments = -1;
	private long numberOfMongoDBIndexes = -1;
	private double totalMongoDBSize = -1;

	private long numberOfOracleCollections = -1;
	private long numberOfOracleJSONDocuments;
	private long numberOfOracleIndexes;
	private double totalOracleSize;

	private final ASCIIProgressBar mainProgressBar;

	private final XYTerminalOutput term;
	private final Timer timer;
	private PoolDataSource pds;

	private List<String> collections = new ArrayList<>();
	private List<ASCIICollectionProgressBar> collectionsProgressBars = new ArrayList<>();
	private ASCIICollectionProgressBar currentCollectionProgressBar;

	public ASCIIGUI(XYTerminalOutput term, String title) {
		this.term = term;
		this.title = title;
		WINDOWS = System.getProperty("os.name").toLowerCase().startsWith("windows");
		mainProgressBar = new ASCIIProgressBar(50, System.currentTimeMillis());
		timer = new Timer("GUI Refresher", true);
	}

	public void write(XYTerminalOutput term) {
		term.moveTo(0, 0);

		// Main Title line 0
		term.write(title).newline();
		// MongoDB information line 1
		term.reset().write(WINDOWS ? "\u00c9 " : "\u2554 ").bold().write(XYTerminalOutput.BrightGreen).write(sourceDatabase).reset().write(" DB");

		if (numberOfMongoDBCollections != -1) {
			term.write(": ");
			term.bold().bright().write(String.format("%,d", numberOfMongoDBCollections)).reset().write(" collection(s), ");
			term.bold().bright().write(String.format("%,d", numberOfMongoDBJSONDocuments)).reset().write(" JSON docs, ");
			term.bold().bright().write(String.format("%,d", numberOfMongoDBIndexes)).reset().write(" index(es), ");
			if (totalMongoDBSize / (1024d * 1024d * 1024d) > 1024d) {
				term.bold().bright().write(String.format("%.1f", totalMongoDBSize / (1024d * 1024d * 1024d * 1024d))).reset().write(" TB").clearToEndOfLine();
			}
			else if (totalMongoDBSize / (1024d * 1024d) > 1024d) {
				term.bold().bright().write(String.format("%.1f", totalMongoDBSize / (1024d * 1024d * 1024d))).reset().write(" GB").clearToEndOfLine();
			}
			else {
				term.bold().bright().write(String.format("%.1f", totalMongoDBSize / (1024d * 1024d))).reset().write(" MB").clearToEndOfLine();
			}
		}
		term.newline();

		// Oracle information line 2
		term.reset().write(WINDOWS ? "\u00c8\u00cd> " : "\u255a\u2550> ").bold().write(BrightRed).write(destinationDatabaseName).reset().write(" DB");

		if (numberOfOracleCollections != -1) {
			term.write(": ");
			term.bold().bright().write(String.format("%,d", numberOfOracleCollections)).reset().write(" collection(s), ");
			term.bold().bright().write(String.format("%,d", numberOfOracleJSONDocuments)).reset().write(" JSON docs, ");
			term.bold().bright().write(String.format("%,d", numberOfOracleIndexes)).reset().write(" index(es), ");
			if (totalOracleSize / (1024d * 1024d * 1024d) > 1024d) {
				term.bold().bright().write(String.format("%.1f", totalOracleSize / (1024d * 1024d * 1024d * 1024d))).reset().write(" TB").clearToEndOfLine();
			}
			else if (totalOracleSize / (1024d * 1024d) > 1024d) {
				term.bold().bright().write(String.format("%.1f", totalOracleSize / (1024d * 1024d * 1024d))).reset().write(" GB").clearToEndOfLine();
			}
			else {
				term.bold().bright().write(String.format("%.1f", totalOracleSize / (1024d * 1024d))).reset().write(" MB").clearToEndOfLine();
			}
		}
		term.newline();

		// Global migration stats line 3
		term.reset().bold().write(XYTerminalOutput.BrightGreen).write("MongoDB").reset().write(" ").write(trailingSpaces(mongoDBVersion, 6)).write(" |");
		mainProgressBar.write(term);
		term.reset().write("| ").bold().write(BrightRed).write("Oracle").reset().write(" ").write(oracleVersion).newline();

		// Now per collection stats line
		for (int i = 0; i < collectionsProgressBars.size(); i++) {
			term.reset();
			if (i == 0 && !collectionsProgressBars.get(i).isFinished()) {
				term.bold().bright();
			}
			term.foreground(TerminalOutput.Color.White).write(forceRightAlignedLength(collections.get(i), 15)).reset().write("|");
			collectionsProgressBars.get(i).write(term);
			term.reset().write("|");

			if (i == 0) {
				if(!collectionsProgressBars.get(i).isFinished()) {
					term.write(" <= pending");
				} else {
					term.write("           ");
				}
			}

			term.newline();
		}
	}

	private String forceRightAlignedLength(String s, int size) {
		if (s.length() > size) {
			return s.substring(0, size);
		}
		else if (s.length() == size) {
			return s;
		}
		else {
			return leadingSpaces(s, size);
		}
	}

	private String trailingSpaces(String s, int expectedLength) {
		final StringBuilder r = new StringBuilder(s);

		while (r.length() < expectedLength) {
			r.append(' ');
		}

		return r.toString();
	}

	private String leadingSpaces(String s, int expectedLength) {
		final StringBuilder r = new StringBuilder();

		while (r.length() + s.length() < expectedLength) {
			r.append(' ');
		}

		r.append(s);
		return r.toString();
	}

	public void setSourceDatabaseName(String sourceDatabase) {
		this.sourceDatabase = sourceDatabase;
	}

	public void setDestinationDatabaseName(String destinationDatabaseName) {
		this.destinationDatabaseName = destinationDatabaseName;
	}

	public String getDestinationDatabaseName() {
		return destinationDatabaseName;
	}

	private long prevBytesReceivedFromClient = -1;
	private long bytesReceivedFromClient;
	private long snap;
	private long prevSnap = -1;
	private float bytesReceivedFromClientRealInMBPerSec;
	private float maxBytesReceivedFromClientRealInMBPerSec = 0;
	private float speed = 0f; // 0 to 63.99

	@Override
	public void run() {
		if (getPds() != null) {
			try (Connection c = getPds().getConnection()) {
				try (PreparedStatement p = c.prepareStatement("SELECT sum(VALUE) FROM gv$sysstat WHERE NAME = 'bytes received via SQL*Net from client'")) {
					try (ResultSet r = p.executeQuery()) {
						if (r.next()) {
							if (prevBytesReceivedFromClient == -1) {
								prevBytesReceivedFromClient = r.getLong(1);
								prevSnap = System.currentTimeMillis();
							}
							else {
								bytesReceivedFromClient = r.getLong(1);
								snap = System.currentTimeMillis();
							}

							bytesReceivedFromClientRealInMBPerSec = ((float) (bytesReceivedFromClient - prevBytesReceivedFromClient) / ((float) (snap - prevSnap) / 1000f)) / (1024f * 1024f);
							prevBytesReceivedFromClient = bytesReceivedFromClient;
							prevSnap = snap;

							if (bytesReceivedFromClientRealInMBPerSec >= 0d) {
								speed = Math.min(32.99f, bytesReceivedFromClientRealInMBPerSec / 16f);
								maxBytesReceivedFromClientRealInMBPerSec = Math.max(maxBytesReceivedFromClientRealInMBPerSec, bytesReceivedFromClientRealInMBPerSec);
								//System.out.println(String.format("%.1f MB/s", (bytesReceivedFromClientReal/(1024f*1024f))));
							}
							else {
								speed = 0;
							}
/*
						final String speedStr = bytesReceivedFromClientRealInMBPerSec <= 1024f ? String.format("%.1f MB/s", bytesReceivedFromClientRealInMBPerSec) : String.format("%.3f GB/s", bytesReceivedFromClientRealInMBPerSec / 1024f);

						TerminalOutput.Color textColor;
						if (speed < (128 / 4f * 0.8f)) {
							textColor = TerminalOutput.Color.Green;
						}
						else if (speed < (128 / 4f * 0.9f)) {
							textColor = TerminalOutput.Color.Yellow;
						}
						else {
							textColor = TerminalOutput.Color.Red;
						}
//MongoDB 5.0.5  [                     0.0 MB/s                     ] Oracle 19.14

						TERM.reset().moveTo(40-(speedStr.length()-5), 2).bold().bright().foreground(textColor).write(speedStr);
*/
							mainProgressBar.setSpeed(bytesReceivedFromClientRealInMBPerSec);
						}
					}
				}
			}
			catch (SQLException ignored) {
			}
		}

		write(term);
	}

	public void start() {
		timer.scheduleAtFixedRate(this, 0, 1000);
	}

	public void stop() {
		timer.cancel();
	}

	public void setsourceDatabaseVersion(String sourceDatabaseVersion) {
		mongoDBVersion = sourceDatabaseVersion;
	}

	public void setDestinationDatabaseVersion(String destinationDatabaseVersion) {
		oracleVersion = destinationDatabaseVersion;
	}

	public void setNumberOfMongoDBCollections(long numberOfMongoDBCollections) {
		this.numberOfMongoDBCollections = numberOfMongoDBCollections;
	}

	public void setNumberOfMongoDBJSONDocuments(long numberOfMongoDBJSONDocuments) {
		this.numberOfMongoDBJSONDocuments = numberOfMongoDBJSONDocuments;
	}

	public void setNumberOfMongoDBIndexes(long numberOfMongoDBIndexes) {
		this.numberOfMongoDBIndexes = numberOfMongoDBIndexes;
	}

	public void setTotalMongoDBSize(double totalMongoDBSize) {
		this.totalMongoDBSize = totalMongoDBSize;
	}

	public void setNumberOfOracleCollections(long numberOfOracleCollections) {
		this.numberOfOracleCollections = numberOfOracleCollections;
	}

	public void setNumberOfOracleJSONDocuments(long numberOfOracleJSONDocuments) {
		this.numberOfOracleJSONDocuments = numberOfOracleJSONDocuments;
	}

	public void setNumberOfOracleIndexes(long numberOfOracleIndexes) {
		this.numberOfOracleIndexes = numberOfOracleIndexes;
	}

	public void setTotalOracleSize(double totalOracleSize) {
		this.totalOracleSize = totalOracleSize;
	}

	public synchronized void updateSourceDatabaseDocuments(long docNumber, long averageDocSize) {
		numberOfMongoDBJSONDocuments += docNumber;
		totalMongoDBSize += docNumber * averageDocSize;

		mainProgressBar.setProgressionPercentage((100d * (double) numberOfOracleJSONDocuments) / (double) numberOfMongoDBJSONDocuments);
		currentCollectionProgressBar.addTargetJSONDocs(docNumber);
	}

	public synchronized void updateDestinationDatabaseDocuments(long docNumber, long totalSize) {
		numberOfOracleJSONDocuments += docNumber;
		totalOracleSize += totalSize;

		mainProgressBar.setProgressionPercentage((100d * (double) numberOfOracleJSONDocuments) / (double) numberOfMongoDBJSONDocuments);
		currentCollectionProgressBar.addJSONDocs(docNumber);
	}

	public synchronized void addNewDestinationDatabaseCollection(String newCollectionName) {
		if (numberOfOracleCollections == -1) {
			numberOfOracleCollections = 1;
		}
		else {
			numberOfOracleCollections++;
		}

		finishLastCollection();
		collections.add(0, newCollectionName);
		collectionsProgressBars.add(0, currentCollectionProgressBar = new ASCIICollectionProgressBar(50, System.currentTimeMillis()));
	}

	public synchronized void finishLastCollection() {
		if (currentCollectionProgressBar != null) {
			currentCollectionProgressBar.finish();
			write(term);
		}
	}

	public synchronized void setPDS(PoolDataSource adminPDS) {
		this.pds = adminPDS;
	}

	public synchronized PoolDataSource getPds() {
		return pds;
	}

	public void addNewDestinationDatabaseIndex() {
		numberOfOracleIndexes++;
	}

	public void flushTerminal() {
		write(term);
		term.moveToBottomScreen(0).reset().newline();
	}
}
