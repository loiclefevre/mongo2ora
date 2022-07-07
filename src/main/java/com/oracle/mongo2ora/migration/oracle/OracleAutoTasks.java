package com.oracle.mongo2ora.migration.oracle;

import oracle.ucp.jdbc.PoolDataSource;

import java.sql.*;

public class OracleAutoTasks {
	private static Boolean dbmsStatsAutoTaskStatus;
	private static Boolean sqlTuningAdvisorAutoTaskStatus;
	private static String automaticIndexingAutoTaskMode;
	private static Boolean automaticIndexingAutoTaskStatus;

	public static void disableIfNeeded(PoolDataSource adminPDS) throws SQLException {
		try (Connection c = adminPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				// https://smarttechways.com/2020/08/18/check-and-change-setting-of-gather-statistics-in-oracle/
				try (ResultSet r = s.executeQuery("select dbms_stats.get_prefs('AUTO_TASK_STATUS') from dual")) {
					if (r.next()) {
						dbmsStatsAutoTaskStatus = r.getString(1).equalsIgnoreCase("ON");
					}
				}

				// https://smarttechways.com/2015/09/03/disable-and-enable-auto-task-job-for-11g-and-12c-version-in-oracle/
				try (ResultSet r = s.executeQuery("SELECT status FROM dba_autotask_client where client_name='sql tuning advisor'")) {
					if (r.next()) {
						sqlTuningAdvisorAutoTaskStatus = r.getString(1).equalsIgnoreCase("ENABLED");
					}
				}

				// https://smarttechways.com/2019/08/06/enable-and-disable-auto-indexing-feature-in-oracle-19/
				try (ResultSet r = s.executeQuery("select parameter_value from dba_auto_index_config where parameter_name='AUTO_INDEX_MODE'")) {
					if (r.next()) {
						automaticIndexingAutoTaskMode = r.getString(1).toUpperCase();
						automaticIndexingAutoTaskStatus = !automaticIndexingAutoTaskMode.equalsIgnoreCase("OFF");
					}
				}

				// DISABLE
				if (dbmsStatsAutoTaskStatus != null && dbmsStatsAutoTaskStatus) {
					// disable High-Frequency Automatic Statistics
					try (CallableStatement cs = c.prepareCall("{call dbms_stats.set_global_prefs('AUTO_TASK_STATUS','OFF')}")) {
						cs.execute();
					}
				}

				if (sqlTuningAdvisorAutoTaskStatus != null && sqlTuningAdvisorAutoTaskStatus) {
					// disable SQL Tuning advisor auto Task
					try (CallableStatement cs = c.prepareCall("{call DBMS_AUTO_TASK_ADMIN.DISABLE(client_name => 'sql tuning advisor', operation => NULL, window_name => NULL )}")) {
						cs.execute();
					}
				}

				if (automaticIndexingAutoTaskStatus != null && automaticIndexingAutoTaskStatus) {
					// disable SQL Tuning advisor auto Task
					try (CallableStatement cs = c.prepareCall("{call DBMS_AUTO_INDEX.CONFIGURE('AUTO_INDEX_MODE','OFF')}")) {
						cs.execute();
					}
				}
			}
		}
	}

	public static void enableIfNeeded(PoolDataSource adminPDS) throws SQLException {
		try (Connection c = adminPDS.getConnection()) {
			if (dbmsStatsAutoTaskStatus != null && dbmsStatsAutoTaskStatus) {
				// enable High-Frequency Automatic Statistics
				try (CallableStatement cs = c.prepareCall("{call dbms_stats.set_global_prefs('AUTO_TASK_STATUS','ON')}")) {
					cs.execute();
				}
			}

			if (sqlTuningAdvisorAutoTaskStatus != null && sqlTuningAdvisorAutoTaskStatus) {
				// enable SQL Tuning advisor auto Task
				try (CallableStatement cs = c.prepareCall("{call DBMS_AUTO_TASK_ADMIN.ENABLE(client_name => 'sql tuning advisor', operation => NULL, window_name => NULL )}")) {
					cs.execute();
				}
			}

			if (automaticIndexingAutoTaskStatus != null && automaticIndexingAutoTaskStatus) {
				// enable SQL Tuning advisor auto Task
				try (CallableStatement cs = c.prepareCall("{call DBMS_AUTO_INDEX.CONFIGURE('AUTO_INDEX_MODE','"+automaticIndexingAutoTaskMode+"')}")) {
					cs.execute();
				}
			}
		}
	}
}
