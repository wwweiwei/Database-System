package org.vanilladb.calvin.storage.metadata;

import org.vanilladb.calvin.sql.PrimaryKey;

public interface PartitionPlan {
	
	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	boolean isFullyReplicated(PrimaryKey key);

	/**
	 * Query the belonging partition.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition
	 */
	int getPartition(PrimaryKey key);

}
