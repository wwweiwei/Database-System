package org.vanilladb.bench.remote.calvin;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.calvin.remote.groupcomm.client.GroupCommConnection;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class CalvinConnection implements SutConnection {
	private GroupCommConnection conn;
	private int connectionId;

	public CalvinConnection(GroupCommConnection conn, int connId) {
		this.conn = conn;
		this.connectionId = connId;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		SpResultSet r = conn.callStoredProc(connectionId, pid, pars);
		return new CalvinSpResultSet(r);
	}

	@Override
	public Connection toJdbcConnection() {
		throw new RuntimeException("ElaSQL does not support JDBC.");
	}
	
	public void sendDirectMessage(int clientId, Serializable message) {
		conn.sendP2pMessageToClientNode(clientId, message);
	}
	
	public int getServerCount() {
		return conn.getServerCount();
	}
	
	public int getClientCount() {
		return conn.getClientCount();
	}
}
