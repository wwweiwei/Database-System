package org.vanilladb.calvin.storage.metadata;

import org.vanilladb.calvin.sql.PrimaryKey;

public class HashPartitionPlan implements PartitionPlan {
	
	private int numOfParts;
	
	public HashPartitionPlan() {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public HashPartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}

	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return false;
	}

	@Override
	public int getPartition(PrimaryKey key) {
		return key.hashCode() % numOfParts;
	}
	
	@Override
	public String toString() {
		return String.format("HashPartitionPlan");
	}
}
