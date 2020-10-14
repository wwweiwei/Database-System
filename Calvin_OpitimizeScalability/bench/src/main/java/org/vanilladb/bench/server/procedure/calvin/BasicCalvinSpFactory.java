package org.vanilladb.bench.server.procedure.calvin;

import org.vanilladb.bench.ControlTransactionType;
import org.vanilladb.calvin.procedure.CalvinStoredProcedure;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;

public class BasicCalvinSpFactory implements CalvinStoredProcedureFactory {
	
	private CalvinStoredProcedureFactory underlayerFactory;
	
	public BasicCalvinSpFactory(CalvinStoredProcedureFactory underlayerFactory) {
		this.underlayerFactory = underlayerFactory;
	}

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		ControlTransactionType txnType = ControlTransactionType.fromProcedureId(pid);
		if (txnType != null) {
			switch (txnType) {
			case START_PROFILING:
				throw new UnsupportedOperationException();
			case STOP_PROFILING:
				throw new UnsupportedOperationException();
			}
		}
		return underlayerFactory.getStoredProcedure(pid, txNum);
	}
}
