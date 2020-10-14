package org.vanilladb.bench.server.procedure.calvin.tpcc;

import java.util.Map;

import org.vanilladb.bench.server.param.tpcc.TpccSchemaBuilderProcParamHelper;
import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.procedure.AllExecuteProcedure;
import org.vanilladb.calvin.schedule.Analyzer;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.core.server.VanillaDb;

public class TpccSchemaBuilderProc extends AllExecuteProcedure<TpccSchemaBuilderProcParamHelper> {

	public TpccSchemaBuilderProc(long txNum) {
		super(txNum, new TpccSchemaBuilderProcParamHelper());
	}

	@Override
	protected void prepareKeys(Analyzer analyzer) {
		// Do nothing
	}

	@Override
	protected void executeSql(Map<PrimaryKey, InMemoryRecord> readings) {
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());

	}
}