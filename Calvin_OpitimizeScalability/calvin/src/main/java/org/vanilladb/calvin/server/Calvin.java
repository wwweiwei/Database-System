package org.vanilladb.calvin.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.cache.CacheMgr;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;
import org.vanilladb.calvin.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.calvin.schedule.Scheduler;
import org.vanilladb.calvin.storage.log.CalvinLogMgr;
import org.vanilladb.calvin.storage.metadata.HashPartitionPlan;
import org.vanilladb.calvin.storage.metadata.NotificationPartitionPlan;
import org.vanilladb.calvin.storage.metadata.PartitionMetaMgr;
import org.vanilladb.calvin.storage.metadata.PartitionPlan;
import org.vanilladb.core.server.VanillaDb;

public class Calvin extends VanillaDb {
	private static Logger logger = Logger.getLogger(Calvin.class.getName());

	public static final long START_TX_NUMBER = 0;
	public static final long START_TIME_MS = System.currentTimeMillis();
	public static final long SYSTEM_INIT_TIME_MS = System.currentTimeMillis();

	// Modules
	private static ConnectionMgr connMgr;
	private static PartitionMetaMgr parMetaMgr;
	private static CacheMgr cacheMgr;
	private static Scheduler scheduler;
	private static CalvinLogMgr requestLogMgr;

	// connection information
	private static int myNodeId;
	
	public static void init(String dirName, int id,
			CalvinStoredProcedureFactory factory) {
		init(dirName, id, factory, new HashPartitionPlan());
	}
	
	public static void init(String dirName, int id,
			CalvinStoredProcedureFactory factory, PartitionPlan partitionPlan) {
		myNodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("Calvin initializing...");

		// initialize core modules
		VanillaDb.init(dirName);

		// initialize DD modules
		initCacheMgr();
		initPartitionMetaMgr(partitionPlan);
		initScheduler(factory);
		initConnectionMgr(myNodeId);
		initDdLogMgr();
	}

	// ================
	// Initializers
	// ================

	public static void initCacheMgr() {
		cacheMgr = new CacheMgr();
		taskMgr().runTask(cacheMgr);
	}

	public static void initScheduler(CalvinStoredProcedureFactory factory) {
		if (!CalvinStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
			throw new IllegalArgumentException("The given factory is not a CalvinStoredProcedureFactory");
		scheduler = new Scheduler(factory);
		taskMgr().runTask(scheduler);
	}
	
	public static void initPartitionMetaMgr(PartitionPlan plan) {
		try {
			// Add a warper partition-meta-mgr for handling notifications
			// between servers
			plan = new NotificationPartitionPlan(plan);
			parMetaMgr = new PartitionMetaMgr(plan);
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}
	}

	public static void initConnectionMgr(int id) {
		connMgr = new ConnectionMgr(id);
	}

	public static void initDdLogMgr() {
		requestLogMgr = new CalvinLogMgr();
	}

	// ================
	// Module Getters
	// ================

	public static CacheMgr cacheMgr() {
		return cacheMgr;
	}

	public static Scheduler scheduler() {
		return scheduler;
	}

	public static PartitionMetaMgr partitionMetaMgr() {
		return parMetaMgr;
	}

	public static ConnectionMgr connectionMgr() {
		return connMgr;
	}

	public static CalvinLogMgr requestLogMgr() {
		return requestLogMgr;
	}

	// ===============
	// Other Getters
	// ===============

	public static int serverId() {
		return myNodeId;
	}
}
