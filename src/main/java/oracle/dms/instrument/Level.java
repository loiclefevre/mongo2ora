package oracle.dms.instrument;

import java.util.Hashtable;
import oracle.core.ojdl.MessageType;

public class Level extends LogLevel {
	public static final Level OFF = initLevel("OFF");
	public static final Level INTERNAL_ERROR = initLevel("INTERNAL_ERROR");
	public static final Level ERROR = initLevel("ERROR");
	public static final Level WARNING = initLevel("WARNING");
	public static final Level NOTIFICATION = initLevel("NOTIFICATION");
	public static final Level TRACE = initLevel("TRACE");
	public static final Level DEBUG = initLevel("DEBUG");
	private MessageType m_type;
	private int m_ArbLevel;
	private int m_value;
	private String m_name;
	private static Hashtable s_allLevels;

	public static Level getLevel(String name) {
		return name == null ? null : (Level)s_allLevels.get(name);
	}

	public String toString() {
		return this.m_name;
	}

	protected Level(String name, MessageType msgType, int ArbLevel, int value) {
		this.m_type = msgType;
		this.m_ArbLevel = ArbLevel;
		this.m_name = name;
		this.m_value = value;
		Class var5 = Level.class;
		synchronized(Level.class) {
			if (s_allLevels == null) {
				s_allLevels = new Hashtable();
			}

			s_allLevels.put(name, this);
		}
	}

	protected final int getValue() {
		return this.m_value;
	}

	protected final MessageType getMessageType() {
		return this.m_type;
	}

	protected final int getArbLevel() {
		return this.m_ArbLevel;
	}

	protected final boolean isGreaterThan(Level level) {
		return this.m_value > level.getValue();
	}

	protected final boolean isLessThanOrEqual(Level level) {
		return this.m_value <= level.getValue();
	}

	static synchronized Level initLevel(String name) {
		if (s_allLevels == null) {
			s_allLevels = new Hashtable();
		}

		Level level = getLevel(name);
		if (level != null) {
			return level;
		} else if (name.equals("OFF")) {
			return new Level("OFF", (MessageType)null, 0, 0);
		} else if (name.equals("INTERNAL_ERROR")) {
			return new Level("INTERNAL_ERROR", MessageType.INTERNAL_ERROR, 1, 1);
		} else if (name.equals("ERROR")) {
			return new Level("ERROR", MessageType.ERROR, 1, 33);
		} else if (name.equals("WARNING")) {
			return new Level("WARNING", MessageType.WARNING, 1, 65);
		} else if (name.equals("NOTIFICATION")) {
			return new Level("NOTIFICATION", MessageType.NOTIFICATION, 1, 97);
		} else if (name.equals("TRACE")) {
			return new Level("TRACE", MessageType.TRACE, 1, 129);
		} else {
			return name.equals("DEBUG") ? new Level("DEBUG", MessageType.TRACE, 17, 145) : null;
		}
	}
}
