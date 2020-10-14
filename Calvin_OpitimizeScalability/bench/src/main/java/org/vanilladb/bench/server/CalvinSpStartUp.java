package org.vanilladb.bench.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.server.metadata.MicroBenchPartitionPlan;
import org.vanilladb.bench.server.metadata.TpccPartitionPlan;
import org.vanilladb.bench.server.procedure.calvin.BasicCalvinSpFactory;
import org.vanilladb.bench.server.procedure.calvin.micro.MicrobenchStoredProcFactory;
import org.vanilladb.bench.server.procedure.calvin.tpcc.TpccStoredProcFactory;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.storage.metadata.PartitionPlan;
import org.vanilladb.core.remote.storedprocedure.SpStartUp;

public class CalvinSpStartUp implements SutStartUp {
	private static Logger logger = Logger.getLogger(VanillaDbSpStartUp.class
			.getName());

	public void startup(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");
		
		Calvin.init(args[0], Integer.parseInt(args[1]),
				getStoredProcedureFactory(), getPartitionPlan());
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Calvin server ready");
	}
	
	private CalvinStoredProcedureFactory getStoredProcedureFactory() {
		CalvinStoredProcedureFactory factory = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			if (logger.isLoggable(Level.INFO))
				logger.info("using Micro-benchmark stored procedures");
			factory = new MicrobenchStoredProcFactory();
			break;
		case TPCC:
			if (logger.isLoggable(Level.INFO))
				logger.info("using TPC-C stored procedures");
			factory = new TpccStoredProcFactory();
			break;
		}
		factory = new BasicCalvinSpFactory(factory);
		return factory;
	}
	
	private PartitionPlan getPartitionPlan() {
		PartitionPlan plan = null;
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			plan = new MicroBenchPartitionPlan();
			break;
		case TPCC:
			plan = new TpccPartitionPlan();
			break;
		}
		return plan;
	}

}
