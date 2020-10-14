package org.vanilladb.calvin.cache;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;

public class CacheMgr extends Task {
	
	private static class HoldPackage {
		long txNum;
		InMemoryRecord record;
		
		HoldPackage(long txNum, InMemoryRecord record) {
			this.txNum = txNum;
			this.record = record;
		}
	}
	
	private Map<Long, TransactionCache> caches = new ConcurrentHashMap<Long, TransactionCache>();
	private LinkedBlockingQueue<HoldPackage> newPacks = new LinkedBlockingQueue<HoldPackage>();
	
	public TransactionCache createCache(Transaction tx) {
		TransactionCache cache = new TransactionCache(tx);
		caches.put(tx.getTransactionNumber(), cache);
		return cache;
	}
	
	public void removeCache(long txNum) {
		caches.remove(txNum);
	}
	
	public void cacheRemoteRecord(long txNum, InMemoryRecord record) {
		if (!handoverToTransaction(txNum, record)) {
			HoldPackage pack = new HoldPackage(txNum, record);
			newPacks.add(pack);
		}
	}

	@Override
	public void run() {
		Queue<HoldPackage> pending = new ArrayDeque<HoldPackage>();
		while (true) {
			try {
				// Check every 1 second (fast enough?)
				HoldPackage pack = newPacks.poll(1, TimeUnit.SECONDS);
				if (pack != null && !handoverToTransaction(pack.txNum, pack.record)) {
					pending.add(pack);
				}
				
				// Handle pending packages
				int count = pending.size();
				for (int i = 0; i < count; i++) {
					pack = pending.poll();
					if (!handoverToTransaction(pack.txNum, pack.record)) {
						pending.add(pack);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private boolean handoverToTransaction(long txNum, InMemoryRecord record) {
		TransactionCache cache = caches.get(txNum);
		if (cache != null) {
			cache.onReceivedRecord(record);
			return true;
		}
		return false;
	}
}
