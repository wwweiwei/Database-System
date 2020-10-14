package org.vanilladb.calvin.procedure;

import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.task.Task;

public class CalvinStoredProcedureTask extends Task {
	
	protected CalvinStoredProcedure<?> sp;
	protected int clientId;
	protected int connectionId;
	protected long txNum;

	public CalvinStoredProcedureTask(int cid, int connId, long txNum, CalvinStoredProcedure<?> sp) {
		this.txNum = txNum;
		this.clientId = cid;
		this.connectionId = connId;
		this.sp = sp;
	}

	public void run() {
		Thread.currentThread().setName("Tx." + txNum + " (running)");

		SpResultSet rs = null;
		rs = sp.execute();

		if (sp.willResponseToClients()) {
			Calvin.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);
		}

		Thread.currentThread().setName("Tx." + txNum + " (committed)");
	}

	public long getTxNum() {
		return txNum;
	}
}
