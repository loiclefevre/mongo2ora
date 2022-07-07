package oracle.core.ojdl;

public class LogManagerInitException extends CheckedLoggingException {
	public LogManagerInitException(String msg) {
		super(msg);
	}

	public LogManagerInitException(Exception exn) {
		super(exn);
	}

	public LogManagerInitException(String msg, Exception exn) {
		super(msg, exn);
	}
}
