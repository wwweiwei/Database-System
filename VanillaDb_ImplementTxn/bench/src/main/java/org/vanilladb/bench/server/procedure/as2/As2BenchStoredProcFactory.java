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
package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.benchmarks.as2.As2BenchTxnType;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory;

public class As2BenchStoredProcFactory implements StoredProcedureFactory {

	@Override
	public StoredProcedure<?> getStroredProcedure(int pid) {
		StoredProcedure<?> sp;
		switch (As2BenchTxnType.fromProcedureId(pid)) {
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc();
			break;
		case CHECK_DATABASE:
			sp = new As2CheckDatabaseProc();
			break;
		case READ_ITEM:
			sp = new ReadItemTxnProc();
			break;
		case UPDATE_ITEM:
			sp = new UpdateItemTxnProc();
			break;
		default:
			throw new IllegalArgumentException("Wrong procedure type");
		}
		return sp;
	}
}
