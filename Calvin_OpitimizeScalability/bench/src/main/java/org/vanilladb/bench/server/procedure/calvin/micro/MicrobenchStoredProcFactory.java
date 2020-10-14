package org.vanilladb.bench.server.procedure.calvin.micro;

import org.vanilladb.bench.benchmarks.micro.MicrobenchTransactionType;
import org.vanilladb.calvin.procedure.CalvinStoredProcedure;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;

public class MicrobenchStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (MicrobenchTransactionType.fromProcedureId(pid)) {
		case TESTBED_LOADER:
			sp = new MicroTestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			throw new UnsupportedOperationException();
		case MICRO_TXN:
			sp = new MicroTxnProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
