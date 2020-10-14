package org.vanilladb.calvin.procedure;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.cache.TransactionCache;
import org.vanilladb.calvin.procedure.ExecutionPlan.ParticipantRole;
import org.vanilladb.calvin.remote.groupcomm.RecordPackage;
import org.vanilladb.calvin.schedule.Analyzer;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.tx.concurrency.ConservativeConcurrencyMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.ManuallyAbortException;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>
		extends StoredProcedure<H> {
	private static Logger logger = Logger.getLogger(CalvinStoredProcedure.class.getName());

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected TransactionCache cache;

	private ExecutionPlan execPlan;
	private Transaction tx;
	private boolean isCommitted = false;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		super(paramHelper);

		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the PrimaryKey for each record to be used in this stored procedure.
	 * Use the {@link #addReadKey(PrimaryKey)}, {@link #addWriteKey(PrimaryKey)}
	 * method to add keys.
	 */
	protected abstract void prepareKeys(Analyzer analyzer);

	protected abstract void executeSql(Map<PrimaryKey, InMemoryRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
		execPlan = analyzeParameters(pars);
		
//		System.out.println("Tx." + txNum + "'s params: " + Arrays.toString(pars));
//		System.out.println("Tx." + txNum + "'s plan: " + execPlan);

		// Prepare a transaction and a cache
		if (isParticipating()) {
			// create a transaction
			tx = Calvin.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, execPlan.isReadOnly(), txNum);

			// create a cache
			cache = Calvin.cacheMgr().createCache(tx);

			// For special transactions
			executeLogicInScheduler(tx);
		}
	}

	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		Analyzer analyzer = new Analyzer();
		prepareKeys(analyzer);

		// generate execution plan
		return analyzer.generatePlan();
	}

	protected void executeLogicInScheduler(Transaction tx) {
		// Prepare for some special transactions (e.g. migration transactions)
	}

	public void bookConservativeLocks() {
		ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
		ccMgr.bookReadKeys(execPlan.getLocalReadKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalUpdateKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalInsertKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalDeleteKeys());
	}

	private void getConservativeLocks() {
		ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
		ccMgr.requestLocks();
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// Execute transaction
			executeTransactionLogic();

			// Flush the cached records
			cache.flush();

			// The transaction finishes normally
			tx.commit();
			isCommitted = true;

			afterCommit();

		} catch (ManuallyAbortException me) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Manually aborted by the procedure: " + me.getMessage());
			tx.rollback();
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("Tx." + txNum + " crashes. The execution plan: " + execPlan);
			e.printStackTrace();
			tx.rollback();
		} finally {
			// Clean the cache
			Calvin.cacheMgr().removeCache(tx.getTransactionNumber());
		}

		return new SpResultSet(isCommitted, paramHelper.getResultSetSchema(), paramHelper.newResultSetRecord());
	}

	public boolean isParticipating() {
		return execPlan.getParticipantRole() != ParticipantRole.IGNORE;
	}

	public boolean willResponseToClients() {
		return execPlan.getParticipantRole() == ParticipantRole.ACTIVE;
	}

	public boolean isReadOnly() {
		return execPlan.isReadOnly();
	}

	@Override
	protected void executeSql() {
		// Do nothing
		// Because we have overrided execute(), there is no need
		// to implement this method.
	}

	/**
	 * This method will be called by execute(). The default implementation of this
	 * method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {
		// Read the local records
		Map<PrimaryKey, InMemoryRecord> readings = performLocalRead(execPlan.getLocalReadKeys());

		// Push local records to the needed remote nodes
		pushRecordsToRemotes(execPlan.getPushSets(), readings);

		// Passive participants stops here
		if (execPlan.getParticipantRole() != ParticipantRole.ACTIVE)
			return;

		// Read the remote records
		collectRemoteReadings(execPlan.getRemoteReadKeys(), readings);

		// Write the local records
		executeSql(readings);
	}

	protected void afterCommit() {
		// Used for clean up or notification.
	}

	protected void update(PrimaryKey key, InMemoryRecord rec) {
		if (execPlan.isLocalUpdate(key))
			cache.update(key, rec);
	}

	protected void insert(PrimaryKey key, Map<String, Constant> fldVals) {
		if (execPlan.isLocalInsert(key))
			cache.insert(key, fldVals);
	}

	protected void delete(PrimaryKey key) {
		if (execPlan.isLocalDelete(key))
			cache.delete(key);
	}

	protected Transaction getTransaction() {
		return tx;
	}

	
	private Map<PrimaryKey, InMemoryRecord> performLocalRead(Set<PrimaryKey> readKeys) {
		Map<PrimaryKey, InMemoryRecord> localReadings = new HashMap<PrimaryKey, InMemoryRecord>();

		// Read local records (for both active or passive participants)
		for (PrimaryKey k : readKeys) {
			InMemoryRecord rec = cache.readFromLocal(k);
			if (rec == null)
				throw new RuntimeException("cannot find the record for " + k + " in the local stroage");
			localReadings.put(k, rec);
		}

		return localReadings;
	}

	//Change to send total message!!!!
	private void pushRecordsToRemotes(Map<Integer, Set<PrimaryKey>> pushKeys, Map<PrimaryKey, InMemoryRecord> records) {
		// RecordPackage pack = new RecordPackage(txNum);
		for (Map.Entry<Integer, Set<PrimaryKey>> entry : pushKeys.entrySet()) {
			Integer targetNodeId = entry.getKey();
			//Integer targetNodeId = entry.getKey();
			Set<PrimaryKey> keys = entry.getValue();
			// Construct a record package
			RecordPackage pack = new RecordPackage(txNum);

			for (PrimaryKey key : keys) {
				InMemoryRecord rec = records.get(key);
				if (rec == null)
					throw new RuntimeException("cannot find the record for " + key);
				pack.addRecord(rec);
			}
			// Push to the target
			Calvin.connectionMgr().pushRecords(targetNodeId, pack);
		}
		// Calvin.connectionMgr().pushRemote(pack);
	}

	private void collectRemoteReadings(Set<PrimaryKey> keys, Map<PrimaryKey, InMemoryRecord> readingCache) {
		// Read remote records
		for (PrimaryKey k : keys) {
			InMemoryRecord rec = cache.readFromRemote(k);
			readingCache.put(k, rec);
		}
	}
}
