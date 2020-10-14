package org.vanilladb.calvin.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class VanillaCoreStorage {

	public static InMemoryRecord read(PrimaryKey key, Transaction tx) {
		String tblName = key.getTableName();
		TablePlan tp = new TablePlan(tblName, tx);
		Plan selectPlan = null;
		
		// Create a IndexSelectPlan if there is matching index in the predicate
		selectPlan = selectByBestMatchedIndex(tblName, tp, key, tx);
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, key.toPredicate());
		else
			selectPlan = new SelectPlan(selectPlan, key.toPredicate());
		
		SelectScan s = (SelectScan) selectPlan.open();
		s.beforeFirst();
		InMemoryRecord rec = null;

		if (s.next()) {
			rec = new InMemoryRecord(key);
			for (String fld : tp.schema().fields())
				rec.addFldVal(fld, s.getVal(fld));
		}
		s.close();
		
		tx.endStatement();

		return rec;
	}

	public static boolean update(PrimaryKey key, InMemoryRecord rec, Transaction tx) {
		boolean found = false;
		String tblName = key.getTableName();
		
		TablePlan tp = new TablePlan(tblName, tx);
		Plan selectPlan = null;
		
		// Create a IndexSelectPlan if there is matching index in the predicate
		selectPlan = selectByBestMatchedIndex(tblName, tp, key, tx, rec.getDirtyFldNames());
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, key.toPredicate());
		else
			selectPlan = new SelectPlan(selectPlan, key.toPredicate());
		
		// Open all indexes associate with target fields
		Set<Index> modifiedIndexes = new HashSet<Index>();
		for (String fieldName : rec.getDirtyFldNames()) {
			List<IndexInfo> iiList = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
			for (IndexInfo ii : iiList)
				modifiedIndexes.add(ii.open(tx));
		}
		
		// Open the scan
		UpdateScan s = (UpdateScan) selectPlan.open();
		s.beforeFirst();
		while (s.next()) {
			found = true;
			
			// Construct a mapping from field names to values
			Map<String, Constant> oldValMap = new HashMap<String, Constant>();
			Map<String, Constant> newValMap = new HashMap<String, Constant>();
			for (String fieldName : rec.getDirtyFldNames()) {
				Constant oldVal = s.getVal(fieldName);
				Constant newVal = rec.getVal(fieldName);
				
				oldValMap.put(fieldName, oldVal);
				newValMap.put(fieldName, newVal);
				s.setVal(fieldName, newVal);
			}
			
			RecordId rid = s.getRecordId();
			
			// Update the indexes
			for (Index index : modifiedIndexes) {
				// Construct a SearchKey for the old value
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant oldVal = oldValMap.get(fldName);
					if (oldVal == null)
						oldVal = s.getVal(fldName);
					fldValMap.put(fldName, oldVal);
				}
				SearchKey oldKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Delete the old value from the index
				index.delete(oldKey, rid, true);
				
				// Construct a SearchKey for the new value
				fldValMap = new HashMap<String, Constant>();
				for (String fldName : index.getIndexInfo().fieldNames()) {
					Constant newVal = newValMap.get(fldName);
					if (newVal == null)
						newVal = s.getVal(fldName);
					fldValMap.put(fldName, newVal);
				}
				SearchKey newKey = new SearchKey(index.getIndexInfo().fieldNames(), fldValMap);
				
				// Insert the new value to the index
				index.insert(newKey, rid, true);
				
				index.close();
			}
		}
		
		// Close opened indexes and the record file
		for (Index index : modifiedIndexes)
			index.close();
		s.close();
		
		tx.endStatement();
		
		return found;
	}

	public static void insert(PrimaryKey key, InMemoryRecord rec, Transaction tx) {
		String tblname = key.getTableName();
		
		Plan p = new TablePlan(tblname, tx);

		// Insert the record into the record file
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
		for (String fldName : rec.getFldNames())
			s.setVal(fldName, rec.getVal(fldName));
		RecordId rid = s.getRecordId();
		s.close();
		
		// Insert the record to all corresponding indexes
		Set<IndexInfo> indexes = new HashSet<IndexInfo>();
		for (String fldname : rec.getFldNames()) {
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblname, fldname, tx);
			indexes.addAll(iis);
		}
		
		for (IndexInfo ii : indexes) {
			Index idx = ii.open(tx);
			idx.insert(new SearchKey(ii.fieldNames(), rec.toFldValMap()), rid, true);
			idx.close();
		}
		
		tx.endStatement();
	}

	public static void delete(PrimaryKey key, Transaction tx) {
		String tblName = key.getTableName();
		TablePlan tp = new TablePlan(tblName, tx);
		Plan selectPlan = null;
		
		// Create a IndexSelectPlan if there is matching index in the predicate
		boolean usingIndex = false;
		selectPlan = selectByBestMatchedIndex(tblName, tp, key, tx);
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, key.toPredicate());
		else {
			selectPlan = new SelectPlan(selectPlan, key.toPredicate());
			usingIndex = true;
		}
		
		// Retrieve all indexes
		List<IndexInfo> allIndexes = new LinkedList<IndexInfo>();
		Set<String> indexedFlds = VanillaDb.catalogMgr().getIndexedFields(tblName, tx);
		
		for (String indexedFld : indexedFlds) {
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, indexedFld, tx);
			allIndexes.addAll(iis);
		}
		
		// Open the scan
		UpdateScan s = (UpdateScan) selectPlan.open();
		s.beforeFirst();
		while (s.next()) {
			RecordId rid = s.getRecordId();
			
			// Delete the record from every index
			for (IndexInfo ii : allIndexes) {
				// Construct a key-value map
				Map<String, Constant> fldValMap = new HashMap<String, Constant>();
				for (String fldName : ii.fieldNames())
					fldValMap.put(fldName, s.getVal(fldName));
				SearchKey searchKey = new SearchKey(ii.fieldNames(), fldValMap);
				
				// Delete from the index
				Index index = ii.open(tx);
				index.delete(searchKey, rid, true);
				index.close();
			}
			
			// Delete the record from the record file
			s.delete();

			/*
			 * Re-open the index select scan to ensure the correctness of
			 * next(). E.g., index block before delete the current slot ^:
			 * [^5,5,6]. After the deletion: [^5,6]. When calling next() of
			 * index select scan, current slot pointer will move forward,
			 * [5,^6].
			 */
			if (usingIndex) {
				s.close();
				s = (UpdateScan) selectPlan.open();
				s.beforeFirst();
			}
		}
		s.close();
		
		tx.endStatement();
	}
	
	private static IndexSelectPlan selectByBestMatchedIndex(String tblName,
			TablePlan tablePlan, PrimaryKey key, Transaction tx) {

		// Look up candidate indexes
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (int i = 0; i < key.getNumOfFlds(); i++) {
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, key.getField(i), tx);
			candidates.addAll(iis);
		}
		
		return selectByBestMatchedIndex(candidates, tablePlan, key, tx);
	}
	
	private static IndexSelectPlan selectByBestMatchedIndex(String tblName,
			TablePlan tablePlan, PrimaryKey key, Transaction tx, Collection<String> excludedFields) {
		
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (int i = 0; i < key.getNumOfFlds(); i++) {
			if (excludedFields.contains(key.getField(i)))
				continue;
			
			List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, key.getField(i), tx);
			for (IndexInfo ii : iis) {
				boolean ignored = false;
				for (String fldName : ii.fieldNames())
					if (excludedFields.contains(fldName)) {
						ignored = true;
						break;
					}
				
				if (!ignored)
					candidates.add(ii);
			}
		}
		
		return selectByBestMatchedIndex(candidates, tablePlan, key, tx);
	}
	
	private static IndexSelectPlan selectByBestMatchedIndex(Set<IndexInfo> candidates,
			TablePlan tablePlan, PrimaryKey key, Transaction tx) {
		
		// Choose the index with the most matched fields in the predicate
		int matchedCount = 0;
		IndexInfo bestIndex = null;
		Map<String, ConstantRange> searchRanges = null;
		
		for (IndexInfo ii : candidates) {
			if (ii.fieldNames().size() < matchedCount)
				continue;
			
			Map<String, ConstantRange> ranges = new HashMap<String, ConstantRange>();
			for (String fieldName : ii.fieldNames()) {
				Constant val = key.getVal(fieldName);
				if (val == null)
					continue;
				
				ranges.put(fieldName, ConstantRange.newInstance(val));
			}
			
			if (ranges.size() > matchedCount) {
				matchedCount = ranges.size();
				bestIndex = ii;
				searchRanges = ranges;
			}
		}
		
		if (bestIndex != null) {
			return new IndexSelectPlan(tablePlan, bestIndex, searchRanges, tx);
		}
		
		return null;
	}
}
