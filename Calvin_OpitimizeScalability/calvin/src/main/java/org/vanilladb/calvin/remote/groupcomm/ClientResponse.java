package org.vanilladb.calvin.remote.groupcomm;

import java.io.Serializable;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ClientResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final int COMMITTED = 0, ROLLED_BACK = 1;

	private long txNum;

	private int clientId, rteId;

	private SpResultSet result;

	public ClientResponse(int clientId, int rteId, long txNum, SpResultSet result) {
		this.txNum = txNum;
		this.clientId = clientId;
		this.rteId = rteId;
		this.result = result;
	}

	public long getTxNum() {
		return txNum;
	}

	public SpResultSet getResultSet() {
		return result;
	}

	public int getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public void setRteId(int rteId) {
		this.rteId = rteId;
	}
}
