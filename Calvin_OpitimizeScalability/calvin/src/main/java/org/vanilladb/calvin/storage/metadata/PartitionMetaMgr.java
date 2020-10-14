package org.vanilladb.calvin.storage.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.util.CalvinProperties;

public class PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(PartitionMetaMgr.class.getName());

	public static final int NUM_PARTITIONS;

	static {
		NUM_PARTITIONS = CalvinProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
	}

	private PartitionPlan partPlan;
	
	public PartitionMetaMgr(PartitionPlan plan) {
		partPlan = plan;
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Using '%s'", partPlan));
	}

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public boolean isFullyReplicated(PrimaryKey key) {
		return partPlan.isFullyReplicated(key);
	}
	
	/**
	 * Get the original location (may not be the current location)
	 * 
	 * @param key
	 * @return
	 */
	public int getPartition(PrimaryKey key) {
		return partPlan.getPartition(key);
	}
}
