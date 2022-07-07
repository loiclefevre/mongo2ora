package oracle.dms.instrument;

public class LogLevel {
	public static final LogLevel OFF = initLogLevel("OFF");
	public static final LogLevel INTERNAL_ERROR = initLogLevel("INTERNAL_ERROR");
	public static final LogLevel ERROR = initLogLevel("ERROR");
	public static final LogLevel WARNING = initLogLevel("WARNING");
	public static final LogLevel NOTIFICATION = initLogLevel("NOTIFICATION");
	public static final LogLevel TRACE = initLogLevel("TRACE");
	public static final LogLevel DEBUG = initLogLevel("DEBUG");

	public LogLevel() {
	}

	public static LogLevel getLogLevel(String name) {
		return Level.getLevel(name);
	}

	private static LogLevel initLogLevel(String name) {
		return Level.initLevel(name);
	}
}
