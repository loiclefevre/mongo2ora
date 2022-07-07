package oracle.core.ojdl;

public class LoggingException extends RuntimeException {
	public LoggingException(String msg) {
		super(msg);
	}

	public LoggingException(Throwable cause) {
		super(cause);
	}

	public LoggingException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/** @deprecated */
	@Deprecated
	public Exception getException() {
		Throwable t = this.getCause();
		return t instanceof Exception ? (Exception)t : null;
	}
}
