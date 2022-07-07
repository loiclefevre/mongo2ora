package oracle.dms.console;

public class DMSError extends Exception {
	public DMSError() {
	}

	public DMSError(String s) {
		super(s);
	}

	public DMSError(Throwable t) {
		super(t);
	}
}
