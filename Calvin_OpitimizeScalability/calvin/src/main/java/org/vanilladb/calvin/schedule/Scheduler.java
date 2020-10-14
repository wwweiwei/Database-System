package org.vanilladb.calvin.schedule;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.procedure.CalvinStoredProcedure;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureTask;
import org.vanilladb.calvin.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.calvin.storage.tx.recovery.CalvinRecoveryMgr;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class Scheduler extends Task {
	private static Logger logger = Logger.getLogger(Scheduler.class.getName());
	
	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();

	public Scheduler(CalvinStoredProcedureFactory factory) {
		this.factory = factory;
	}

	public void schedule(StoredProcedureCall call) {
		try {
			spcQueue.put(call);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		StoredProcedureCall call = null;
		try {
			while (true) {
				// retrieve stored procedure call
				call = spcQueue.take();
	
				// create store procedure and prepare
				CalvinStoredProcedure<?> sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				
				sp.prepare(call.getPars());
	
				// log request
				if (!sp.isReadOnly()) {
					CalvinRecoveryMgr recoveryMgr = new CalvinRecoveryMgr();
					recoveryMgr.logRequest(call);
				}
	
				// if this node doesn't have to participate this transaction,
				// skip it
				if (!sp.isParticipating()) {
					continue;
				}
	
				// serialize conservative locking
				sp.bookConservativeLocks();
	
				// create a new task for multi-thread
				CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
						call.getClientId(), call.getConnectionId(), call.getTxNum(),
						sp);
	
				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("detect Exception in the scheduler, current sp call: " + call);
			e.printStackTrace();
		}
	}
}
