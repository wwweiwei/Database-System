package org.vanilladb.calvin.remote.groupcomm;

import java.io.Serializable;
import java.util.Arrays;

public class StoredProcedureCall implements Serializable {

	private static final long serialVersionUID = 8807383803517134106L;

	private Object[] objs;

	private long txNum = -1;

	private int clientId, pid, connectionId = -1;

	public StoredProcedureCall(int clientId, int connId, int pid, Object... objs) {
		this.clientId = clientId;
		this.connectionId = connId;
		this.pid = pid;
		this.objs = objs;
	}

	public Object[] getPars() {
		return objs;
	}

	public long getTxNum() {
		return txNum;
	}

	public void setTxNum(long txNum) {
		this.txNum = txNum;
	}

	public int getClientId() {
		return clientId;
	}

	public int getConnectionId() {
		return connectionId;
	}

	public int getPid() {
		return pid;
	}
	
	@Override
	public String toString() {
		return "{Tx." + txNum + ", pars: " + Arrays.toString(objs) + ", issued client " +
					clientId + " on connection " + connectionId + "}";
	}
}
