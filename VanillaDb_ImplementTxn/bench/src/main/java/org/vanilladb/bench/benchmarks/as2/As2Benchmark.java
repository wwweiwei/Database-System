/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.benchmarks.as2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.vanilladb.bench.Benchmark;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.BenchTransactionType;
import org.vanilladb.bench.benchmarks.as2.rte.As2BenchRte;
import org.vanilladb.bench.benchmarks.as2.rte.As2BenchTxExecutor;
import org.vanilladb.bench.benchmarks.as2.rte.TestbedLoaderParamGen;
import org.vanilladb.bench.benchmarks.as2.rte.jdbc.As2BenchJdbcExecutor;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class As2Benchmark extends Benchmark {
	
	@Override
	public Set<BenchTransactionType> getBenchmarkingTxTypes() {
		Set<BenchTransactionType> txTypes = new HashSet<BenchTransactionType>();
		for (BenchTransactionType txType : As2BenchTxnType.values()) {
			if (txType.isBenchmarkingProcedure())
				txTypes.add(txType);
		}
		return txTypes;
	}
	
	@Override
	public void executeLoadingProcedure(SutConnection conn) throws SQLException {
		As2BenchTxExecutor loader = new As2BenchTxExecutor(new TestbedLoaderParamGen());
		loader.execute(conn);
	}
	
	@Override
	public RemoteTerminalEmulator<As2BenchTxnType> createRte(SutConnection conn, StatisticMgr statMgr) {
		return new As2BenchRte(conn, statMgr);
	}

	@Override
	public boolean executeDatabaseCheckProcedure(SutConnection conn) throws SQLException {
		SutResultSet result = null;
		As2BenchTxnType txnType = As2BenchTxnType.CHECK_DATABASE;
		Object[] params = new Object[] {As2BenchConstants.NUM_ITEMS};
		
		switch (BenchmarkerParameters.CONNECTION_MODE) {
		case JDBC:
			Connection jdbcConn = conn.toJdbcConnection();
			jdbcConn.setAutoCommit(false);
			As2BenchJdbcExecutor executor = new As2BenchJdbcExecutor();
			result = executor.execute(jdbcConn, txnType, params);
			break;
		case SP:
			result = conn.callStoredProc(txnType.getProcedureId(), params);
			break;
		}
		
		return result.isCommitted();
	}

	@Override
	public String getBenchmarkName() {
		
		return "as2bench";
	}
}
