package org.vanilladb.bench.server.metadata;

import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.metadata.PartitionMetaMgr;
import org.vanilladb.calvin.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class TpccPartitionPlan implements PartitionPlan {
	
	private static final int TOTAL_WAREHOUSES = TpccConstants.NUM_WAREHOUSES * PartitionMetaMgr.NUM_PARTITIONS;

	public boolean isFullyReplicated(PrimaryKey key) {
		return key.getTableName().equals("item");
	}
	
	public static Integer getWarehouseId(PrimaryKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getVal("w_id");
			break;
		case "district":
			widCon = key.getVal("d_w_id");
			break;
		case "stock":
			widCon = key.getVal("s_w_id");
			break;
		case "customer":
			widCon = key.getVal("c_w_id");
			break;
		case "history":
			widCon = key.getVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getVal("ol_w_id");
			break;
		default:
			return null;
		}
		
		return (Integer) widCon.asJavaVal();
	}
	
	public int numOfWarehouses() {
		return TOTAL_WAREHOUSES;
	}
	
	public int getPartition(int wid) {
		return (wid - 1) / TpccConstants.NUM_WAREHOUSES;
	}
	
	@Override
	public int getPartition(PrimaryKey key) {
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return Calvin.serverId();
		
		Integer wid = getWarehouseId(key);
		if (wid != null) {
			return getPartition(wid);
		} else {
			// Fully replicated
			return Calvin.serverId();
		}
	}
	
	public String toString() {
		return String.format("TPC-C range partition (each range has %d warehouses)", TpccConstants.NUM_WAREHOUSES);
	}
}
