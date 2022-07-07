package oracle.core.ojdl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

public class LogManager {
	private static final String PROPERTIES_PREFIX = "oracle.core.ojdl.";
	private static final int MAX_UNIQUE_ID_LEN = 64;
	private String m_processId = null;
	private String m_encoding = checkEncoding((String)null);
	private ExceptionHandlerIntf m_exnHandler = new ExceptionHandler(false);
	private Properties m_properties = null;
	private long m_initCount = 0L;
	private Vector m_writers = null;
	private LogWriter m_globalLogWriter;
	private String m_startId;
	private boolean m_debug = false;
	private static LogManager s_globalManager = new LogManager();
	private static String s_alternateProcessId = null;

	protected LogManager() {
	}

	public static LogManager getLogManager() {
		return s_globalManager;
	}

	public synchronized void init(Properties properties) throws LogManagerInitException {
		if (++this.m_initCount <= 1L) {
			this.m_properties = properties;
			if (this.getProperty("Debug", "false").equals("true")) {
				this.m_exnHandler = new ExceptionHandler(true);
				this.m_debug = true;
			} else {
				this.m_exnHandler = new ExceptionHandler(false);
				this.m_debug = false;
			}

			String pid = this.getProperty("ProcessId");
			if (pid != null) {
				this.setProcessId(pid);
			}

			this.m_startId = null;
			String enc = this.getProperty("Encoding");
			this.m_encoding = checkEncoding(enc);
			this.m_writers = new Vector();
			LogMessage.initDefaultValues();
		}
	}

	public synchronized void term() {
		if (--this.m_initCount <= 0L) {
			this.m_properties = null;
			this.m_processId = null;
			this.m_exnHandler = null;
			if (this.m_globalLogWriter != null) {
				this.m_globalLogWriter.close();
				this.m_globalLogWriter = null;
			}

			if (this.m_writers != null) {
				for(int i = 0; i < this.m_writers.size(); ++i) {
					((LogWriter)((LogWriter)this.m_writers.elementAt(i))).close();
				}

				this.m_writers = null;
			}

		}
	}

	public synchronized void setExceptionHandler(ExceptionHandlerIntf handler) {
		this.m_exnHandler = handler;
	}

	public ExceptionHandlerIntf getExceptionHandler() {
		return this.m_exnHandler;
	}

	public String getUniqueId() {
		if (this.m_startId == null) {
			String hostAddr = null;

			try {
				hostAddr = InetAddress.getLocalHost().getHostAddress();
			} catch (Exception var4) {
				hostAddr = "";
			}

			String procId = this.getProcessId();
			if (procId == null) {
				procId = s_alternateProcessId;
			}

			this.m_startId = hostAddr + ":" + procId;
			int maxPrefixLen = 24;
			if (this.m_startId.length() > maxPrefixLen) {
				this.m_startId = this.m_startId.substring(0, maxPrefixLen);
			}

			this.m_startId = this.m_startId + ":";
		}

		return this.m_startId + System.currentTimeMillis() + ":" /*+ UniqueCounter.next()*/;
	}

	public synchronized void setProcessId(String processId) {
		this.m_processId = processId;
	}

	public String getProcessId() {
		return this.m_processId;
	}

	public long getProcessUniqueValue() {
		return 0/*UniqueCounter.next()*/;
	}

	public String getEncoding() {
		return this.m_encoding;
	}

	public synchronized void setGlobalLogWriter(LogWriter writer) {
		this.m_globalLogWriter = writer;
	}

	public LogWriter getGlobalLogWriter() {
		return this.m_globalLogWriter;
	}

	static String checkEncoding(String enc) {
		OutputStreamWriter osw;
		if (enc != null) {
			try {
				osw = new OutputStreamWriter(new ByteArrayOutputStream(0), enc);
				return osw.getEncoding();
			} catch (Exception var2) {
				return null;
			}
		} else {
			try {
				osw = new OutputStreamWriter(new ByteArrayOutputStream(0));
				return osw.getEncoding();
			} catch (Exception var3) {
				return null;
			}
		}
	}

	private boolean isActive() {
		return this.m_initCount > 0L;
	}

	String getProperty(String key, String defaultValue) {
		return this.m_properties == null ? defaultValue : this.m_properties.getProperty("oracle.core.ojdl." + key, defaultValue);
	}

	String getProperty(String key) {
		return this.getProperty(key, (String)null);
	}

	static String getSystemProperty(final String key) {
		return (String)AccessController.doPrivileged(new PrivilegedAction<String>() {
			public String run() {
				return System.getProperty(key);
			}
		});
	}

	synchronized void addLogWriter(LogWriter writer) {
		if (this.isActive()) {
			this.m_writers.addElement(writer);
		}

	}

	synchronized void removeLogWriter(LogWriter writer) {
		if (this.isActive() && !this.m_writers.removeElement(writer)) {
			this.getExceptionHandler().onException(new LoggingException("Attempt to remove nonexistant LogWriter"));
		}

	}

	boolean getDebugMode() {
		return this.m_debug;
	}

	void debug(String str) {
		if (this.m_debug) {
			System.err.println(str);
		}

	}

	static {
		try {
			Random rand = new Random();
			s_alternateProcessId = Integer.toString(rand.nextInt(100000));
		} catch (Exception var1) {
		}

	}
}
