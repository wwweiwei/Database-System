package org.vanilladb.calvin.remote.groupcomm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.calvin.cache.InMemoryRecord;

public class RecordPackage implements Serializable {
	
	private static final long serialVersionUID = 3191495851408477607L;
	
	private long txNum;
	private List<InMemoryRecord> records;

	public RecordPackage(long txNum) {
		this.txNum = txNum;
		records = new ArrayList<InMemoryRecord>();
	}

	public void addRecord(InMemoryRecord rec) {
		// Clone the record to prevent concurrent access from communication threads
		rec = new InMemoryRecord(rec);
		records.add(rec);
	}
	
	public List<InMemoryRecord> getRecords() {
		return records;
	}
	
	public int size() {
		return records.size();
	}
	
	public long getTxNum() {
		return txNum;
	}
}
