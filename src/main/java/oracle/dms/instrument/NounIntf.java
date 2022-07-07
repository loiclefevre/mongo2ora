package oracle.dms.instrument;

import java.io.PrintWriter;
import java.util.Properties;
import java.util.Vector;
import oracle.core.ojdl.LogWriter;

public interface NounIntf {
	String UNKNOWN_TYPE = "n/a";
	int TYPE_LEN = 511;
	byte DIRECT = 1;
	byte ALL = 2;

	String getName();

	Vector getNouns();

	void setType(String var1);

	String getType();

	void destroy();

	void setLogLevel(Level var1, boolean var2);

	void setLogLevel(LogLevel var1, boolean var2);

	Level getLogLevel();

	void addLogWriter(LogWriter var1);

	void removeLogWriter(LogWriter var1);

	LogWriter[] getLogWriters();

	void setLoggingProperties(Properties var1);

	void dump(PrintWriter var1, String var2);

	void dump(String var1, String var2, boolean var3);

	Sensor getSensor(String var1);
}
