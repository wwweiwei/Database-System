package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.server.param.as2.TestbedLoaderParamHelper;

public class CheckDatabaseJdbcJob implements JdbcJob {

private static Logger logger = Logger.getLogger(CheckDatabaseJdbcJob.class.getName());
	
	TestbedLoaderParamHelper paramHelper;

	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// Parse parameters
		paramHelper = new TestbedLoaderParamHelper();
		paramHelper.prepareParameters(pars);
		
		// Execute logic
		try {
			Statement stat = conn.createStatement();
			if (checkItemTable(stat, 1, paramHelper.getNumberOfItems()))
				conn.commit();
			else 
				conn.rollback();
			
			return new VanillaDbJdbcResultSet(true, "Success");
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.warning(e.toString());
			conn.rollback();
			return new VanillaDbJdbcResultSet(false, "");
		}
	}

	private boolean checkItemTable(Statement stat, int startIId, int endIId) throws SQLException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Checking items from i_id=" + startIId + " to i_id=" + endIId);
		
		// Use a bit array to record existence
		int total = endIId - startIId + 1;
		boolean[] checked = new boolean[total];
		for (int i = 0; i < total; i++)
			checked[i] = false;
		
		// Scan the table
		String sql = "SELECT i_id FROM item";
		ResultSet resultSet = stat.executeQuery(sql);
		resultSet.beforeFirst();
		for (int i = startIId, count = 0; i <= endIId; i++) {
			if (!resultSet.next()) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Only %d records are found (there should be %d records)",
							count, total));
				return false;
			}
			
			int id = resultSet.getInt("i_id");
			if (checked[id - 1]) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Found duplicated record (i_id = %d)", id));
				return false;
			}
			checked[id - 1] = true;
			count++;
		}
		resultSet.close();

		if (logger.isLoggable(Level.FINE))
			logger.info("Checking items completed.");
		
		return true;
	}

}
