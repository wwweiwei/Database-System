package org.vanilladb.bench.remote.calvin;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.calvin.remote.groupcomm.client.DirectMessageListener;
import org.vanilladb.calvin.remote.groupcomm.client.GroupCommConnection;
import org.vanilladb.calvin.remote.groupcomm.client.GroupCommDriver;

public class CalvinSpDriver implements SutDriver {
	
	private static final AtomicInteger NEXT_CONNECTION_ID = new AtomicInteger(0);
	
	private static GroupCommConnection conn = null;
	
	public CalvinSpDriver(int nodeId, DirectMessageListener messageListener) {
		if (conn == null) {
			GroupCommDriver driver = new GroupCommDriver(nodeId);
			conn = driver.connect(messageListener);
		}
	}

	public SutConnection connectToSut() throws SQLException {
		try {
			// Each connection need a unique id
			return new CalvinConnection(conn, NEXT_CONNECTION_ID.getAndIncrement());
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
}
