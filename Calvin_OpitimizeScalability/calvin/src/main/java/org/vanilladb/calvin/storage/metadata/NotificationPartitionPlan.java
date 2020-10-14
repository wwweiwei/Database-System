package org.vanilladb.calvin.storage.metadata;

import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartitionPlan implements PartitionPlan {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SENDER_NAME = "sender_node_id";
	public static final String KEY_RECV_NAME = "recv_node_id";
	
	public static PrimaryKey createRecordKey(int sender, int reciever) {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder(TABLE_NAME);
		builder.addFldVal(KEY_SENDER_NAME, new IntegerConstant(sender));
		builder.addFldVal(KEY_RECV_NAME, new IntegerConstant(reciever));
		return builder.build();
	}
	
	public static InMemoryRecord createRecord(int sender, int reciever, long txNum) {
		// Create a record
		PrimaryKey key = createRecordKey(sender, reciever);
		InMemoryRecord rec = new InMemoryRecord(key);
		rec.setTempRecord(true);
		return rec;
	}
	
	private PartitionPlan underlayerPlan;
	
	public NotificationPartitionPlan(PartitionPlan plan) {
		underlayerPlan = plan;
	}
	
	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return false;
		
		return underlayerPlan.isFullyReplicated(key);
	}

	@Override
	public int getPartition(PrimaryKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return -1; // Not belongs to anyone, preventing for inserting to local
		
		return underlayerPlan.getPartition(key);
	}
	
	public PartitionPlan getUnderlayerPlan() {
		return underlayerPlan;
	}
	
	@Override
	public String toString() {
		return String.format("Notification Partition Plan (underlayer: %s)", underlayerPlan.toString());
	}
}
