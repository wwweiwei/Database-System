package org.vanilladb.calvin.storage.tx.recovery;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public class StoredProcRequestRecord implements LogRecord {
	
	public static final int OP_SP_REQUEST = -99999;
	
	private long txNum;
	private int clientId, connectionId, procedureId;
	private Object[] pars;
	private LogSeqNum lsn;
	
	/**
	 * 
	 * Creates a new stored procedure request log record for the specified
	 * transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param cid
	 * @param pid
	 * @param pars
	 */
	public StoredProcRequestRecord(long txNum, int cid, int connId, int pid,
			Object... pars) {
		this.txNum = txNum;
		this.clientId = cid;
		this.connectionId = connId;
		this.procedureId = pid;
		this.pars = pars;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public StoredProcRequestRecord(BasicLogRecord rec) {
		// TODO: Deserialize the record
		throw new UnsupportedOperationException();
	}
	
	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return Calvin.requestLogMgr().append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_SP_REQUEST;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	@Override
	public void redo(Transaction tx) {
		// TODO: Replay the transaction
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "<SP_REQUEST " + txNum + " " + procedureId + " " + clientId + 
				" " + Arrays.toString(pars) + " >";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new IntegerConstant(clientId));
		rec.add(new IntegerConstant(connectionId));
		rec.add(new IntegerConstant(procedureId));
		rec.add(new IntegerConstant(pars.length));
		rec.add(new VarcharConstant(Arrays.toString(pars)));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}
}
