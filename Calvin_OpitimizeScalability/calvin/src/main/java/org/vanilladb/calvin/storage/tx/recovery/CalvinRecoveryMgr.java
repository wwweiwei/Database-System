package org.vanilladb.calvin.storage.tx.recovery;

import org.vanilladb.calvin.remote.groupcomm.StoredProcedureCall;

public class CalvinRecoveryMgr {
	public void logRequest(StoredProcedureCall spc) {
		new StoredProcRequestRecord(spc.getTxNum(), spc.getClientId(), spc.getConnectionId(), spc.getPid(),
				spc.getPars()).writeToLog();
	}
}
