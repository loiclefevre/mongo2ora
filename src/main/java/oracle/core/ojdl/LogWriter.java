package oracle.core.ojdl;

public abstract class LogWriter {
	private ExceptionHandlerIntf m_exnHandler = null;

	protected LogWriter() {
		this.m_exnHandler = LogManager.getLogManager().getExceptionHandler();
	}

	public void close() {
		this.flush();
	}

	public void flush() {
	}

	public abstract void write(LogMessage var1);

	public void write(LogMessage message, boolean flush) {
		this.write(message);
		if (flush) {
			this.flush();
		}

	}

	public void setExceptionHandler(ExceptionHandlerIntf handler) {
		this.m_exnHandler = handler;
	}

	public ExceptionHandlerIntf getExceptionHandler() {
		return this.m_exnHandler != null ? this.m_exnHandler : LogManager.getLogManager().getExceptionHandler();
	}
}
