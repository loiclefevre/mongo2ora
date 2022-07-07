package oracle.dms.console;

public abstract class DMSConsole {
	/** @deprecated */
	@Deprecated
	protected DMSConsole() {
	}

	public static synchronized DMSConsole getConsole() {
		return null;
	}

	public abstract void init(String var1) throws DMSError;

}
