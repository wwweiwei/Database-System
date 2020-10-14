package org.vanilladb.calvin.procedure;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.procedure.ExecutionPlan.ParticipantRole;
import org.vanilladb.calvin.remote.groupcomm.RecordPackage;
import org.vanilladb.calvin.schedule.Analyzer;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.metadata.NotificationPartitionPlan;
import org.vanilladb.calvin.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper> extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class.getName());

	private static final String KEY_FINISH = "finish";

	private static final int MASTER_NODE = 0;

	private int localNodeId = Calvin.serverId();

	public AllExecuteProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);
	}

	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		Analyzer analyzer = new Analyzer();
		prepareKeys(analyzer);

		// generate execution plan
		return alterExecutionPlan(analyzer.generatePlan());
	}

	@Override
	public boolean willResponseToClients() {
		// The master node is the only one that will response to the clients.
		return localNodeId == MASTER_NODE;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	protected void prepareKeys(Analyzer analyzer) {
		// default: do nothing
	}

	private ExecutionPlan alterExecutionPlan(ExecutionPlan plan) {
		if (localNodeId == MASTER_NODE) {
			for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
				plan.addRemoteReadKey(NotificationPartitionPlan.createRecordKey(nodeId, MASTER_NODE));
		}
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		plan.setForceReadWriteTx();

		return plan;
	}

	@Override
	protected void executeTransactionLogic() {
		executeSql(null);

		// Notification for finish
		if (localNodeId == MASTER_NODE) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting for other servers...");

			// Master: Wait for notification from other nodes
			waitForNotification();

			if (logger.isLoggable(Level.INFO))
				logger.info("Other servers completion comfirmed.");
		} else {
			// Salve: Send notification to the master
			sendNotification();
		}
	}

	private void waitForNotification() {
		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Waiting for the notification from node no." + nodeId);

				PrimaryKey notKey = NotificationPartitionPlan.createRecordKey(nodeId, MASTER_NODE);
				InMemoryRecord rec = cache.readFromRemote(notKey);
				Constant con = rec.getVal(KEY_FINISH);
				int value = (int) con.asJavaVal();
				if (value != 1)
					throw new RuntimeException("Notification value error, node no." + nodeId + " sent " + value);

				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive notification from node no." + nodeId);
			}
	}

	private void sendNotification() {
		// Create a record
		InMemoryRecord notRec = NotificationPartitionPlan.createRecord(Calvin.serverId(), MASTER_NODE, txNum);
		notRec.addFldVal(KEY_FINISH, new IntegerConstant(1));

		// Use node id as source tx number
		RecordPackage pack = new RecordPackage(txNum);
		pack.addRecord(notRec);
		Calvin.connectionMgr().pushRecords(0, pack);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}
}
