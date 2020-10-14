package org.vanilladb.bench.server.procedure.calvin.tpcc;

import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.calvin.procedure.CalvinStoredProcedure;
import org.vanilladb.calvin.procedure.CalvinStoredProcedureFactory;

public class TpccStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TpccTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new TpccSchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TpccTestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			throw new UnsupportedOperationException();
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
