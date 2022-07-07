package oracle.core.ojdl;

import java.io.Serializable;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogMessage implements Serializable {
	static final int MAX_LEVEL = 32;
	public static final String ORGANIZATION_ID = "ORG_ID";
	public static final String COMPONENT_ID = "COMPONENT_ID";
	public static final String INSTANCE_ID = "INSTANCE_ID";
	public static final String MESSAGE_ID = "MSG_ID";
	public static final String HOSTING_CLIENT_ID = "HOSTING_CLIENT_ID";
	public static final String MESSAGE_TYPE = "MSG_TYPE";
	public static final String MESSAGE_GROUP = "MSG_GROUP";
	public static final String MESSAGE_LEVEL = "MSG_LEVEL";
	public static final String MODULE_ID = "MODULE_ID";
	public static final String PROCESS_ID = "PROCESS_ID";
	public static final String THREAD_ID = "THREAD_ID";
	public static final String USER_ID = "USER_ID";
	public static final String SUPPL_ATTRS = "SUPPL_ATTRS";
	public static final String SUPPL_ATTR_NAME = "SUPPL_ATTR.NAME";
	public static final String SUPPL_ATTR_VALUE = "SUPPL_ATTR.VALUE";
	public static final String UPSTREAM_COMPONENT_ID = "UPSTREAM_COMPONENT_ID";
	public static final String DOWNSTREAM_COMPONENT_ID = "DOWNSTREAM_COMPONENT_ID";
	public static final String EXEC_CONTEXT_UNIQUE_ID = "EXEC_CONTEXT_UNIQUE_ID";
	/** @deprecated */
	@Deprecated
	public static final String EXEC_CONTEXT_SEQ = "EXEC_CONTEXT_SEQ";
	public static final String EXEC_CONTEXT_RID = "EXEC_CONTEXT_RID";
	public static final String ERROR_UNIQUE_ID = "ERROR_UNIQUE_ID";
	public static final String ERROR_SEQ = "ERROR_SEQ";
	public static final String MESSAGE_TEXT = "MSG_TEXT";
	public static final String MSG_ARGS = "MSG_ARGS";
	public static final String MSG_ARG_NAME = "NAME";
	public static final String MSG_ARG_VALUE = "VALUE";
	public static final String DETAIL_LOCATION = "DETAIL_PATH";
	public static final String SUPPL_DETAIL = "SUPPL_DETAIL";
	public static final String TSTZ = "TSTZ_ORIGINATING";
	public static final String TSTZ_ORIGINATING = "TSTZ_ORIGINATING";
	public static final String TSTZ_ORIGINATING_STR = "TSTZ_ORIGINATING_STR";
	public static final String TSTZ_NORMALIZED = "TSTZ_NORMALIZED";
	public static final String HOST_ID = "HOST_ID";
	public static final String HOST_NWADDR = "HOST_NWADDR";
	public static final String PROBLEM_KEY = "PROB_KEY";
	public static final String SUPPL_ATTR_PREFIX = "SUPPL_ATTR.";
	static final long serialVersionUID = -189888222135417808L;
	private static final String ORG_ID_PROP = "OrganizationId";
	private static final String COMP_ID_PROP = "ComponentId";
	private static final String HOST_CLIENT_ID_PROP = "HostingClientId";
	private static final String INSTANCE_ID_PROP = "InstanceId";
	private static HashMap s_messageProperties;
	private static String s_defaultOrganizationId = null;
	private static String s_defaultComponentId = null;
	private static String s_defaultHostingClientId = null;
	private static String s_hostId = null;
	private static String s_hostNwAddr = null;
	private static String s_defaultUserId = null;
	private static String s_defaultInstanceId = null;
	private long m_timestamp;
	private long m_timestampNorm;
	private String m_organizationId;
	private String m_componentId;
	private String m_instanceId;
	private String m_messageId;
	private String m_hostingClientId;
	private MessageType m_messageType;
	private String m_messageGroup;
	private int m_messageLevel;
	private String m_hostId;
	private String m_hostNwAddr;
	private String m_moduleId;
	private String m_processId;
	private String m_userId;
	private String m_problemKey;
	private Map m_supplAttrs;
	private String m_upstreamCompId;
	private String m_downstreamCompId;
	private LogMessage.InstanceId m_execContextId;
	private LogMessage.InstanceId m_errorInstanceId;
	private String m_messageText;
	private LogMessage.MessageArgument[] m_messageArgs;
	private String m_detailLocation;
	private String m_supplDetail;
	private String m_threadId;

	public LogMessage(String organizationId, String componentId, String messageId, String hostingClientId, MessageType messageType, String messageGroup, int messageLevel, String moduleId, String processId, String userId, String upstreamCompId, String downstreamCompId, LogMessage.InstanceId execContextId, LogMessage.InstanceId errorInstanceId, String messageText, LogMessage.MessageArgument[] messageArgs, String detailLocation, String supplDetail) {
		this.m_timestamp = System.currentTimeMillis();
		if (organizationId != null) {
			this.m_organizationId = organizationId;
		} else {
			this.m_organizationId = s_defaultOrganizationId;
		}

		if (componentId != null) {
			this.m_componentId = componentId;
		} else if (s_defaultComponentId != null) {
			this.m_componentId = s_defaultComponentId;
		} else {
			LogManager.getLogManager().getExceptionHandler().onException(new LoggingException("Missing value for componentId"));
			this.m_componentId = null;
		}

		this.m_instanceId = s_defaultInstanceId;
		this.m_messageId = messageId;
		if (hostingClientId != null) {
			this.m_hostingClientId = hostingClientId;
		} else {
			this.m_hostingClientId = s_defaultHostingClientId;
		}

		this.m_messageType = messageType;
		this.m_messageGroup = messageGroup;
		if (messageLevel >= 1 && messageLevel <= 32) {
			this.m_messageLevel = messageLevel;
		} else {
			LogManager.getLogManager().getExceptionHandler().onException(new LoggingException("Invalid message level"));
			this.m_messageLevel = 16;
		}

		this.m_hostId = s_hostId;
		this.m_hostNwAddr = s_hostNwAddr;
		this.m_moduleId = moduleId;
		if (processId != null) {
			this.m_processId = processId;
			this.m_threadId = null;
		} else {
			this.m_processId = getDefaultProcessId();
			this.m_threadId = Thread.currentThread().getName();
		}

		if (userId != null) {
			this.m_userId = userId;
		} else {
			this.m_userId = s_defaultUserId;
		}

		this.m_upstreamCompId = upstreamCompId;
		this.m_downstreamCompId = downstreamCompId;
		this.m_execContextId = execContextId;
		this.m_errorInstanceId = errorInstanceId;
		this.m_messageText = messageText;
		this.m_messageArgs = messageArgs;
		this.m_detailLocation = detailLocation;
		this.m_supplDetail = supplDetail;
	}

	public LogMessage() {
		this(true);
	}

	public LogMessage(boolean initDefaults) {
		if (initDefaults) {
			this.m_timestamp = System.currentTimeMillis();
			this.m_organizationId = s_defaultOrganizationId;
			this.m_componentId = s_defaultComponentId;
			this.m_instanceId = s_defaultInstanceId;
			this.m_hostingClientId = s_defaultHostingClientId;
			this.m_messageType = MessageType.UNKNOWN;
			this.m_messageLevel = 16;
			this.m_processId = getDefaultProcessId();
			this.m_threadId = Thread.currentThread().getName();
			this.m_hostId = s_hostId;
			this.m_hostNwAddr = s_hostNwAddr;
			this.m_userId = s_defaultUserId;
		} else {
			this.m_messageLevel = 1;
		}

	}

	public LogMessage(Properties props) {
		if (props == null) {
			this.m_organizationId = s_defaultOrganizationId;
			this.m_componentId = s_defaultComponentId;
			this.m_hostingClientId = s_defaultHostingClientId;
			this.m_messageType = MessageType.UNKNOWN;
			this.m_messageLevel = 16;
			this.m_processId = getDefaultProcessId();
			this.m_threadId = Thread.currentThread().getName();
			this.m_userId = s_defaultUserId;
		} else {
			StringBuffer supplDet = null;
			String str = props.getProperty("TSTZ_ORIGINATING");
			if (str == null) {
				this.m_timestamp = System.currentTimeMillis();
			} else {
				try {
					this.m_timestamp = Long.parseLong(str);
				} catch (Exception var18) {
					this.m_timestamp = System.currentTimeMillis();
					supplDet = new StringBuffer(str);
				}
			}

			str = props.getProperty("TSTZ_NORMALIZED");
			if (str != null) {
				try {
					this.m_timestampNorm = Long.parseLong(str);
				} catch (Exception var17) {
					this.m_timestampNorm = 0L;
				}
			}

			this.m_hostId = props.getProperty("HOST_ID");
			if (this.m_hostId == null) {
				this.m_hostId = s_hostId;
			}

			this.m_hostNwAddr = props.getProperty("HOST_NWADDR");
			if (this.m_hostNwAddr == null) {
				this.m_hostNwAddr = s_hostNwAddr;
			}

			this.m_organizationId = props.getProperty("ORG_ID");
			if (this.m_organizationId == null) {
				this.m_organizationId = s_defaultOrganizationId;
			}

			this.m_componentId = props.getProperty("COMPONENT_ID");
			if (this.m_componentId == null) {
				this.m_componentId = s_defaultComponentId;
			}

			this.m_instanceId = props.getProperty("INSTANCE_ID");
			if (this.m_instanceId == null) {
				this.m_instanceId = s_defaultInstanceId;
			}

			this.m_messageId = props.getProperty("MSG_ID");
			this.m_hostingClientId = props.getProperty("HOSTING_CLIENT_ID");
			if (this.m_hostingClientId == null) {
				this.m_hostingClientId = s_defaultHostingClientId;
			}

			String typeName = props.getProperty("MSG_TYPE");
			if (typeName != null) {
				this.m_messageType = MessageType.getMessageType(typeName);
			} else {
				this.m_messageType = MessageType.UNKNOWN;
			}

			this.m_messageGroup = props.getProperty("MSG_GROUP");
			String msgLevel = props.getProperty("MSG_LEVEL");

			try {
				this.m_messageLevel = Integer.parseInt(msgLevel);
			} catch (Exception var16) {
				this.m_messageLevel = 16;
			}

			if (this.m_messageLevel <= 0 || this.m_messageLevel > 32) {
				this.m_messageLevel = 16;
			}

			this.m_moduleId = props.getProperty("MODULE_ID");
			this.m_processId = props.getProperty("PROCESS_ID");
			if (this.m_processId == null) {
				this.m_processId = getDefaultProcessId();
			}

			this.m_threadId = props.getProperty("THREAD_ID");
			if (this.m_threadId == null) {
				this.m_threadId = Thread.currentThread().getName();
			}

			this.m_userId = props.getProperty("USER_ID");
			if (this.m_userId == null) {
				this.m_userId = s_defaultUserId;
			}

			this.m_problemKey = props.getProperty("PROB_KEY");
			this.m_upstreamCompId = props.getProperty("UPSTREAM_COMPONENT_ID");
			this.m_downstreamCompId = props.getProperty("DOWNSTREAM_COMPONENT_ID");
			String exec = props.getProperty("EXEC_CONTEXT_UNIQUE_ID");
			String errUId;
			if (exec != null) {
				errUId = props.getProperty("EXEC_CONTEXT_SEQ");
				if (errUId == null) {
					errUId = props.getProperty("EXEC_CONTEXT_SEQ");
				}

				this.m_execContextId = new LogMessage.InstanceId(exec, errUId);
			}

			errUId = props.getProperty("ERROR_UNIQUE_ID");
			int i;
			if (errUId != null) {
				String strSeq = props.getProperty("ERROR_SEQ");
				if (strSeq != null) {
					try {
						i = Integer.parseInt(strSeq);
					} catch (Exception var15) {
						i = 0;
					}
				} else {
					i = 0;
				}

				this.m_errorInstanceId = new LogMessage.InstanceId(errUId, i);
			}

			this.m_messageText = props.getProperty("MSG_TEXT");
			this.m_detailLocation = props.getProperty("DETAIL_PATH");
			str = props.getProperty("SUPPL_DETAIL");
			if (str != null) {
				if (supplDet != null) {
					supplDet.append(str);
				} else {
					supplDet = new StringBuffer(str);
				}
			}

			ArrayList msgArgs = null;
			i = 0;

			while(true) {
				String name = "MSG_ARGS." + i + "." + "VALUE";
				String value = props.getProperty(name);
				if (value == null) {
					if (msgArgs != null) {
						LogMessage.MessageArgument[] args = new LogMessage.MessageArgument[msgArgs.size()];
						this.setMessageArgs((LogMessage.MessageArgument[])((LogMessage.MessageArgument[])msgArgs.toArray(args)));
					}

					Enumeration propNames = props.propertyNames();

					while(propNames.hasMoreElements()) {
						name = (String)propNames.nextElement();
						if (!s_messageProperties.containsKey(name) && !name.startsWith("MSG_ARGS")) {
							if (this.m_supplAttrs == null) {
								this.m_supplAttrs = new HashMap();
							}

							this.m_supplAttrs.put(name, props.getProperty(name));
						}
					}

					if (supplDet != null) {
						this.m_supplDetail = supplDet.toString();
					} else {
						this.m_supplDetail = null;
					}

					return;
				}

				String nameProp = "MSG_ARGS." + i + "." + "NAME";
				name = props.getProperty(nameProp);
				LogMessage.MessageArgument arg = new LogMessage.MessageArgument(name, value);
				if (msgArgs == null) {
					msgArgs = new ArrayList();
				}

				msgArgs.add(arg);
				++i;
			}
		}
	}

	public void setTimestamp(long timestamp) {
		this.m_timestamp = timestamp;
	}

	public void setNormalizedTimestamp(long timestamp) {
		this.m_timestampNorm = timestamp;
	}

	public void setOrganizationId(String organizationId) {
		this.m_organizationId = organizationId;
	}

	public void setComponentId(String componentId) {
		this.m_componentId = componentId;
	}

	public void setInstanceId(String instanceId) {
		this.m_instanceId = instanceId;
	}

	public void setMessageId(String messageId) {
		this.m_messageId = messageId;
	}

	public void setHostingClientId(String hostingClientId) {
		this.m_hostingClientId = hostingClientId;
	}

	public void setMessageType(MessageType messageType) {
		this.m_messageType = messageType;
	}

	public void setMessageGroup(String messageGroup) {
		this.m_messageGroup = messageGroup;
	}

	public void setMessageLevel(int messageLevel) {
		if (messageLevel >= 1 && messageLevel <= 32) {
			this.m_messageLevel = messageLevel;
		} else {
			LogManager.getLogManager().getExceptionHandler().onException(new LoggingException("Invalid message level"));
			this.m_messageLevel = 16;
		}

	}

	public void setHostId(String hostId) {
		this.m_hostId = hostId;
	}

	public void setHostNwAddr(String hostNwAddr) {
		this.m_hostNwAddr = hostNwAddr;
	}

	public void setModuleId(String moduleId) {
		this.m_moduleId = moduleId;
	}

	public void setProcessId(String processId) {
		this.m_processId = processId;
	}

	public void setThreadId(String threadId) {
		this.m_threadId = threadId;
	}

	public void setUserId(String userId) {
		this.m_userId = userId;
	}

	public void setProblemKey(String problemKey) {
		this.m_problemKey = problemKey;
	}

	public void setSupplAttrs(Map supplAttrs) {
		this.m_supplAttrs = supplAttrs;
	}

	public void setUpstreamCompId(String upstreamCompId) {
		this.m_upstreamCompId = upstreamCompId;
	}

	public void setDownstreamCompId(String downstreamCompId) {
		this.m_downstreamCompId = downstreamCompId;
	}

	public void setExecContextId(LogMessage.InstanceId execContextId) {
		this.m_execContextId = execContextId;
	}

	public void setErrorInstanceId(LogMessage.InstanceId errorInstanceId) {
		this.m_errorInstanceId = errorInstanceId;
	}

	public void setMessageText(String messageText) {
		this.m_messageText = messageText;
	}

	public void setMessageArgs(LogMessage.MessageArgument[] messageArgs) {
		this.m_messageArgs = messageArgs;
	}

	public void setDetailLocation(String detailLocation) {
		this.m_detailLocation = detailLocation;
	}

	public void setSupplementalDetail(String supplementalDetail) {
		this.m_supplDetail = supplementalDetail;
	}

	public long getTimestamp() {
		return this.m_timestamp;
	}

	public long getNormalizedTimestamp() {
		return this.m_timestampNorm;
	}

	public String getOrganizationId() {
		return this.m_organizationId;
	}

	public String getComponentId() {
		return this.m_componentId;
	}

	public String getInstanceId() {
		return this.m_instanceId;
	}

	public String getMessageId() {
		return this.m_messageId;
	}

	public String getHostingClientId() {
		return this.m_hostingClientId;
	}

	public MessageType getMessageType() {
		return this.m_messageType;
	}

	public String getMessageGroup() {
		return this.m_messageGroup;
	}

	public int getMessageLevel() {
		return this.m_messageLevel;
	}

	public String getHostId() {
		return this.m_hostId;
	}

	public String getHostNwAddr() {
		return this.m_hostNwAddr;
	}

	public String getModuleId() {
		return this.m_moduleId;
	}

	public String getProcessId() {
		return this.m_processId;
	}

	public String getUserId() {
		return this.m_userId;
	}

	public String getProblemKey() {
		return this.m_problemKey;
	}

	public Map getSupplAttrs() {
		return this.m_supplAttrs;
	}

	public String getUpstreamCompId() {
		return this.m_upstreamCompId;
	}

	public String getDownstreamCompId() {
		return this.m_downstreamCompId;
	}

	public LogMessage.InstanceId getExecContextId() {
		return this.m_execContextId;
	}

	public LogMessage.InstanceId getErrorInstanceId() {
		return this.m_errorInstanceId;
	}

	public String getMessageText() {
		return this.m_messageText;
	}

	public LogMessage.MessageArgument[] getMessageArgs() {
		return this.m_messageArgs;
	}

	public String getDetailLocation() {
		return this.m_detailLocation;
	}

	public String getSupplDetail() {
		return this.m_supplDetail;
	}

	public String getThreadId() {
		return this.m_threadId;
	}

	static void initDefaultValues() {
		LogManager mgr = LogManager.getLogManager();
		s_defaultOrganizationId = mgr.getProperty("OrganizationId", (String)null);
		s_defaultComponentId = mgr.getProperty("ComponentId", (String)null);
		s_defaultHostingClientId = mgr.getProperty("HostingClientId", (String)null);
		s_defaultInstanceId = mgr.getProperty("InstanceId", (String)null);
	}

	private static void init() {
		try {
			s_hostId = (String)AccessController.doPrivileged(new PrivilegedExceptionAction<String>() {
				public String run() throws Exception {
					return InetAddress.getLocalHost().getHostName();
				}
			});
		} catch (Exception var3) {
			LogManager.getLogManager().getExceptionHandler().onException(var3);
			s_hostId = null;
		}

		try {
			s_hostNwAddr = (String)AccessController.doPrivileged(new PrivilegedExceptionAction<String>() {
				public String run() throws Exception {
					return InetAddress.getLocalHost().getHostAddress();
				}
			});
		} catch (Exception var2) {
			LogManager.getLogManager().getExceptionHandler().onException(var2);
			s_hostNwAddr = null;
		}

		try {
			s_defaultUserId = LogManager.getSystemProperty("user.name");
		} catch (Exception var1) {
			s_defaultUserId = null;
		}

		s_messageProperties = new HashMap();
		s_messageProperties.put("ORG_ID", "");
		s_messageProperties.put("COMPONENT_ID", "");
		s_messageProperties.put("INSTANCE_ID", "");
		s_messageProperties.put("MSG_ID", "");
		s_messageProperties.put("HOSTING_CLIENT_ID", "");
		s_messageProperties.put("MSG_TYPE", "");
		s_messageProperties.put("MSG_GROUP", "");
		s_messageProperties.put("MSG_LEVEL", "");
		s_messageProperties.put("MODULE_ID", "");
		s_messageProperties.put("PROCESS_ID", "");
		s_messageProperties.put("THREAD_ID", "");
		s_messageProperties.put("USER_ID", "");
		s_messageProperties.put("UPSTREAM_COMPONENT_ID", "");
		s_messageProperties.put("DOWNSTREAM_COMPONENT_ID", "");
		s_messageProperties.put("EXEC_CONTEXT_UNIQUE_ID", "");
		s_messageProperties.put("EXEC_CONTEXT_RID", "");
		s_messageProperties.put("ERROR_UNIQUE_ID", "");
		s_messageProperties.put("ERROR_SEQ", "");
		s_messageProperties.put("MSG_TEXT", "");
		s_messageProperties.put("DETAIL_PATH", "");
		s_messageProperties.put("SUPPL_DETAIL", "");
		s_messageProperties.put("TSTZ_ORIGINATING", "");
		s_messageProperties.put("TSTZ_NORMALIZED", "");
		s_messageProperties.put("HOST_ID", "");
		s_messageProperties.put("HOST_NWADDR", "");
		s_messageProperties.put("PROB_KEY", "");
	}

	static String getDefaultProcessId() {
		return LogManager.getLogManager().getProcessId();
	}

	static {
		init();
	}

	public static class InstanceId implements Serializable {
		private String m_uid;
		private String m_rid;
		private int m_sequenceNum;

		/** @deprecated */
		@Deprecated
		public InstanceId(String uniqueId, int sequenceNum) {
			this(uniqueId, String.valueOf(sequenceNum), sequenceNum);
		}

		public InstanceId(String uniqueId, String rid) {
			this(uniqueId, rid, 0);
		}

		InstanceId(String uniqueId, String rid, int sequenceNum) {
			this.m_uid = uniqueId;
			this.m_rid = rid;
			this.m_sequenceNum = sequenceNum;
		}

		public String getUniqueId() {
			return this.m_uid;
		}

		public String getRID() {
			return this.m_rid;
		}

		/** @deprecated */
		@Deprecated
		public int getSequenceNumber() {
			return this.m_sequenceNum;
		}
	}

	public static class MessageArgument implements Serializable {
		private String m_name;
		private String m_value;

		public MessageArgument(String name, String value) {
			this.m_name = name;
			this.m_value = value;
		}

		public String getName() {
			return this.m_name;
		}

		public String getValue() {
			return this.m_value;
		}
	}
}
