package com.oracle.mongo2ora.migration.oracle;

import oracle.jdbc.internal.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.*;

public class MediumServiceManager {
	private static int shares = -1;
	private static int concurrencyLimit = -1;
	private static int ocpu = -1;
	private static Boolean autoScalingEnabled = null;

	public static PoolDataSource configure(PoolDataSource adminPDS, PoolDataSource userPDS, String userPassword) throws SQLException {
		try (Connection c = adminPDS.getConnection()) {
			try (Statement s = c.createStatement()) {
				try (ResultSet r = s.executeQuery("SELECT sum(value) FROM gv$parameter where name='cpu_count'")) {
					if (r.next()) {
						ocpu = r.getInt(1);
					}
				}
			}

			try (Statement s = c.createStatement()) {
				try (ResultSet r = s.executeQuery("SELECT SHARES, CONCURRENCY_LIMIT FROM CS_RESOURCE_MANAGER.LIST_CURRENT_RULES() where consumer_group = 'MEDIUM'")) {
					if (r.next()) {
						shares = r.getInt(1);
						concurrencyLimit = r.getInt(2);
					}
				}

				try (CallableStatement cs = c.prepareCall("{call CS_RESOURCE_MANAGER.UPDATE_PLAN_DIRECTIVE(consumer_group => 'MEDIUM', shares => 12, CONCURRENCY_LIMIT => 1)}")) {
					cs.execute();
				}
				catch(SQLException sqle) {
					if(sqle.getErrorCode() == 20000) {
						autoScalingEnabled = ocpu == 6;
					} else {
						throw sqle;
					}
				}
			}
			catch(SQLException sqle) {
				if(sqle.getErrorCode() != 904) {
					throw sqle;
				}
			}


			// Creating new connection pool with MEDIUM service
			final PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
			pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");

			String serviceName = "?";

			try (Statement s = c.createStatement()) {
				try (ResultSet r = s.executeQuery("SELECT sys_context('USERENV','SERVICE_NAME') from dual")) {
					if (r.next()) {
						serviceName = r.getString(1);
					}
				}
			}

			if("?".equals(serviceName)) throw new IllegalStateException("Unable to get current service name!");

			// System.out.println("Service name: " + serviceName);

			final String newServiceName = serviceName.replace("_tpurgent.","_medium.")
					.replace("_tp.","_medium.")
					.replace("_low.","_medium.")
					.replace("_high.","_medium.");

			pds.setURL(userPDS.getURL().toLowerCase().replace(serviceName.toLowerCase(),newServiceName.toLowerCase()));

			//System.out.println("New URL (should user medium service): " + pds.getURL());

			pds.setUser(userPDS.getUser());
			pds.setPassword(userPassword);
			pds.setConnectionPoolName("USER_MEDIUM_JDBC_UCP_POOL-" + Thread.currentThread().getName());
			pds.setInitialPoolSize(3);
			pds.setMinPoolSize(3);
			pds.setMaxPoolSize(3);
			pds.setTimeoutCheckInterval(120);
			pds.setInactiveConnectionTimeout(120);
			pds.setValidateConnectionOnBorrow(true);
			pds.setMaxStatements(20);
			pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
			pds.setConnectionProperty("oracle.jdbc.bindUseDBA", "true");
			pds.setConnectionProperty("oracle.jdbc.thinForceDNSLoadBalancing", "true");

			return pds;
		}
	}


	public static void restore(PoolDataSource adminPDS) throws SQLException {
		if (shares != -1 && concurrencyLimit != -1) {
			try (Connection c = adminPDS.getConnection()) {
				try (CallableStatement cs = c.prepareCall("{call CS_RESOURCE_MANAGER.UPDATE_PLAN_DIRECTIVE(consumer_group => 'MEDIUM', shares => ?, CONCURRENCY_LIMIT => ?)}")) {
					cs.setInt(1, shares);
					cs.setInt(2, concurrencyLimit);
					cs.execute();
				}
				catch(SQLException sqle) {
					if(sqle.getErrorCode() != 20000) {
						throw sqle;
					}
				}
			}
		}
	}
}
