package org.vanilladb.bench.server.procedure.calvin.micro;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.procedure.CalvinStoredProcedure;
import org.vanilladb.calvin.schedule.Analyzer;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroTxnProc extends CalvinStoredProcedure<MicroTxnProcParamHelper> {

	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();

	public MicroTxnProc(long txNum) {
		super(txNum, new MicroTxnProcParamHelper());
	}

	@Override
	public void prepareKeys(Analyzer analyzer) {
		// set read keys
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addReadKey(key);
		}

		// set write keys
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			
			// create record key for writing
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addUpdateKey(key);

			// Create key-value pairs for writing
			Constant c = new DoubleConstant(newPrice);
			writeConstantMap.put(key, c);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, InMemoryRecord> readings) {
		// SELECT i_name, i_price FROM items WHERE i_id = ...
		int idx = 0;
		for (InMemoryRecord rec : readings.values()) {
			paramHelper.setItemName((String) rec.getVal("i_name").asJavaVal(), idx);
			paramHelper.setItemPrice((double) rec.getVal("i_price").asJavaVal(), idx++);
		}

		// UPDATE items SET i_price = ... WHERE i_id = ...
		for (Map.Entry<PrimaryKey, Constant> pair : writeConstantMap.entrySet()) {
			InMemoryRecord rec = readings.get(pair.getKey());
			rec.setVal("i_price", pair.getValue());
			update(pair.getKey(), rec);
		}

	}
}
