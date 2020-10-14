package org.vanilladb.bench.server.metadata;

import org.vanilladb.bench.benchmarks.micro.MicrobenchConstants;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionPlan implements PartitionPlan {
	
	public Integer getItemId(PrimaryKey key) {
		Constant iidCon = key.getVal("i_id");
		if (iidCon != null) {
			return (Integer) iidCon.asJavaVal();
		} else {
			return null;
		}
	}
	
	public boolean isFullyReplicated(PrimaryKey key) {
		if (key.getVal("i_id") != null) {
			return false;
		} else {
			return true;
		}
	}
	
	public int getPartition(int iid) {
		return (iid - 1) / MicrobenchConstants.NUM_ITEMS;
	}
	
	public int getPartition(PrimaryKey key) {
		Integer iid = getItemId(key);
		if (iid != null) {
			return getPartition(iid);
		} else {
			// Fully replicated
			return Calvin.serverId();
		}
	}
	
	public String toString() {
		return String.format("Microbenchmark range partition (each range has %d items)", MicrobenchConstants.NUM_ITEMS);
	}
}
