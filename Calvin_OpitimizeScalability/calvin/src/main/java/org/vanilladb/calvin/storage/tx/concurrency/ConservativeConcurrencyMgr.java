package org.vanilladb.calvin.storage.tx.concurrency;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.tx.concurrency.ConservativeLockTable.LockType;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public class ConservativeConcurrencyMgr extends ConcurrencyMgr {
	protected static ConservativeLockTable lockTbl = new ConservativeLockTable();
	
	// For normal operations - using conservative locking 
	private Set<Object> bookedObjs, readObjs, writeObjs;

	// For Indexes - using crabbing locking
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	public ConservativeConcurrencyMgr(long txNumber) {
		txNum = txNumber;
		bookedObjs = new HashSet<Object>();
		readObjs = new HashSet<Object>();
		writeObjs = new HashSet<Object>();
	}
	
	public void bookReadKey(PrimaryKey key) {
		if (key != null) {
			// The key needs to be booked only once. 
			if (!bookedObjs.contains(key))
				lockTbl.requestLock(key, txNum);
			
			bookedObjs.add(key);
			readObjs.add(key);
		}
	}

	/**
	 * Book the read lock of the specified objects.
	 * 
	 * @param keys
	 *            the objects which the transaction intends to read
	 */
	public void bookReadKeys(Collection<PrimaryKey> keys) {
		if (keys != null) {
			for (PrimaryKey key : keys) {
				// The key needs to be booked only once. 
				if (!bookedObjs.contains(key))
					lockTbl.requestLock(key, txNum);
			}
			
			bookedObjs.addAll(keys);
			readObjs.addAll(keys);
		}
	}
	
	public void bookWriteKey(PrimaryKey key) {
		if (key != null) {
			// The key needs to be booked only once. 
			if (!bookedObjs.contains(key))
				lockTbl.requestLock(key, txNum);
			
			bookedObjs.add(key);
			writeObjs.add(key);
		}
	}
	
	/**
	 * Book the write lock of the specified object.
	 * 
	 * @param keys
	 *             the objects which the transaction intends to write
	 */
	public void bookWriteKeys(Collection<PrimaryKey> keys) {
		if (keys != null) {
			for (PrimaryKey key : keys) {
				// The key needs to be booked only once. 
				if (!bookedObjs.contains(key))
					lockTbl.requestLock(key, txNum);
			}
			
			bookedObjs.addAll(keys);
			writeObjs.addAll(keys);
		}
	}
	
	/**
	 * Request (get the locks immediately) the locks which the transaction
	 * has booked. If the locks can not be obtained in the time, it will
	 * make the thread wait until it can obtain all locks it requests.
	 */
	public void requestLocks() {
		bookedObjs.clear();
		
		for (Object obj : writeObjs)
			lockTbl.xLock(obj, txNum);
		
		for (Object obj : readObjs)
			if (!writeObjs.contains(obj))
				lockTbl.sLock(obj, txNum);
	}
	
	@Override
	public void onTxCommit(Transaction tx) {
		releaseIndexLocks();
		releaseLocks();
	}
	
	@Override
	public void onTxRollback(Transaction tx) {
		releaseIndexLocks();
		releaseLocks();
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// Next-key lock algorithm is non-deterministic. It may
		// cause deadlocks during the execution. Therefore,
		// we release the locks earlier to prevent deadlocks.
		// However, phantoms due to update may happen.
		releaseIndexLocks();
	}

	@Override
	public void modifyFile(String fileName) {
		// do nothing
	}

	@Override
	public void readFile(String fileName) {
		// do nothing
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void readBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void insertBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// do nothing
	}

	@Override
	public void readIndex(String dataFileName) {
		// do nothing
	}

	/*
	 * Methods for B-Tree index locking
	 */

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum, LockType.X_LOCK);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum, LockType.S_LOCK);
		readIndexBlks.remove(blk);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.release(blk, txNum, LockType.S_LOCK);
		for (BlockId blk : writtenIndexBlks)
			lockTbl.release(blk, txNum, LockType.X_LOCK);
		readIndexBlks.clear();
		writtenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum, LockType.X_LOCK);
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// do nothing
	}

	@Override
	public void readRecord(RecordId recId) {
		// do nothing
	}
	
	private void releaseLocks() {
		for (Object obj : writeObjs)
			lockTbl.release(obj, txNum, LockType.X_LOCK);
		
		for (Object obj : readObjs)
			if (!writeObjs.contains(obj))
				lockTbl.release(obj, txNum, LockType.S_LOCK);
		
		readObjs.clear();
		writeObjs.clear();
	}
	
	// opt
	
	// release readLock after flush
	public void releaseReadLock(Object obj) {
		lockTbl.release(obj, txNum, LockType.S_LOCK);
		for (Object i : readObjs) {
			if (obj.equals(i))
				readObjs.remove(i);
		}
	}
	
	// release writeLock after flush
	public void releaseWriteLock(Object obj) {	
		lockTbl.release(obj, txNum, LockType.X_LOCK);
		/*
		for (Object i : writeObjs) {
			if (obj.equals(i)) {
				System.out.println("i:"+i);
				writeObjs.remove(i);
			}
		}
		*/
	}
	
	// opt
	
}
