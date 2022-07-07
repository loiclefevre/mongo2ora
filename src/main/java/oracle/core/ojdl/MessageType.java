package oracle.core.ojdl;

import java.io.ObjectStreamException;
import java.io.Serializable;

public class MessageType implements Serializable {
	public static final MessageType INCIDENT_ERROR = new MessageType("INCIDENT_ERROR");
	/** @deprecated */
	@Deprecated
	public static final MessageType INTERNAL_ERROR;
	public static final MessageType ERROR;
	public static final MessageType WARNING;
	public static final MessageType NOTIFICATION;
	public static final MessageType TRACE;
	public static final MessageType UNKNOWN;
	private String m_name;

	protected MessageType(String name) {
		this.m_name = name;
	}

	public String toString() {
		return this.m_name;
	}

	public static MessageType getMessageType(String typeName) {
		if (typeName.equals("TRACE")) {
			return TRACE;
		} else if (typeName.equals("NOTIFICATION")) {
			return NOTIFICATION;
		} else if (typeName.equals("WARNING")) {
			return WARNING;
		} else if (typeName.equals("ERROR")) {
			return ERROR;
		} else {
			return !typeName.equals("INCIDENT_ERROR") && !typeName.equals("INTERNAL_ERROR") ? UNKNOWN : INCIDENT_ERROR;
		}
	}

	private Object readResolve() throws ObjectStreamException {
		return getMessageType(this.m_name);
	}

	static {
		INTERNAL_ERROR = INCIDENT_ERROR;
		ERROR = new MessageType("ERROR");
		WARNING = new MessageType("WARNING");
		NOTIFICATION = new MessageType("NOTIFICATION");
		TRACE = new MessageType("TRACE");
		UNKNOWN = new MessageType("UNKNOWN");
	}
}
