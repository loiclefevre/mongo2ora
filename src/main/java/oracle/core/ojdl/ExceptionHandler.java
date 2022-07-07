package oracle.core.ojdl;

public class ExceptionHandler implements ExceptionHandlerIntf {
	private Exception m_lastException = null;
	private boolean m_throwExceptions = false;

	public ExceptionHandler(boolean throwExceptions) {
		this.m_throwExceptions = throwExceptions;
	}

	public void onException(Exception exn) {
		this.m_lastException = exn;
		if (this.m_throwExceptions) {
			if (exn instanceof LoggingException) {
				throw (LoggingException)exn;
			} else {
				throw new LoggingException(exn);
			}
		}
	}

	public Exception getLastException() {
		return this.m_lastException;
	}

	public void reset() {
		this.m_lastException = null;
	}
}
