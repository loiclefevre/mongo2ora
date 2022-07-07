package oracle.core.ojdl;

public class CheckedLoggingException extends Exception {
	public CheckedLoggingException(String msg) {
		super(msg);
	}

	public CheckedLoggingException(Throwable cause) {
		super(cause);
	}

	public CheckedLoggingException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/** @deprecated */
	@Deprecated
	public Exception getException() {
		Throwable t = this.getCause();
		return t instanceof Exception ? (Exception)t : null;
	}
}
