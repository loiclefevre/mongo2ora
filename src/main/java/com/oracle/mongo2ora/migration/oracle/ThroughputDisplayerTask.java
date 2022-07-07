package com.oracle.mongo2ora.migration.oracle;

import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimerTask;

public class ThroughputDisplayerTask extends TimerTask {
	private final XYTerminalOutput TERM;
	private final PoolDataSource pds;

	private long prevBytesReceivedFromClient = -1;
	private long bytesReceivedFromClient;
	private long snap;
	private long prevSnap = -1;
	private float bytesReceivedFromClientRealInMBPerSec;
	private float maxBytesReceivedFromClientRealInMBPerSec = 0;
	private float speed = 0f; // 0 to 63.99

	public ThroughputDisplayerTask(PoolDataSource pds, XYTerminalOutput term) {
		this.pds = pds;
		this.TERM = term;
	}

	@Override
	public void run() {
		try (Connection c = pds.getConnection()) {
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

					}
				}
			}
		}
		catch (SQLException ignored) {
		}
	}
}