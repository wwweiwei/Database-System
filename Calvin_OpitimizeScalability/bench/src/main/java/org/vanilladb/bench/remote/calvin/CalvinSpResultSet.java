package org.vanilladb.bench.remote.calvin;

import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Record;

public class CalvinSpResultSet implements SutResultSet {
	
	private SpResultSet result;
	private String message;

	public CalvinSpResultSet(SpResultSet result) {
		this.result = result;
	}

	@Override
	public boolean isCommitted() {
		return result.isCommitted();
	}
	
	@Override
	public String outputMsg() {
		// Lazy evaluation
		if (message == null) {
			Record[] records = result.getRecords();
			if (records.length > 0)
				message = records[0].toString();
			else
				message = "";
		}
		
		return message;
	}
}
