package org.vanilladb.calvin.storage.tx.concurrency;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class ConservativeLockTable {
	
	enum LockType {
		S_LOCK, X_LOCK
	}

	private class Lockers {
		static final long NONE = -1; // for sixLocker, xLocker and wbLocker
		
		List<Long> sLockers;
		// only one tx can hold xLock on single item
		long xLocker;
		Queue<Long> requestQueue;

		Lockers() {
			sLockers = new LinkedList<Long>();
			xLocker = NONE;
			requestQueue = new LinkedList<Long>();
		}
		
		@Override
		public String toString() {
			return "{S: " + sLockers +
					", X: " + xLocker +
					", requests: " + requestQueue +
					"}";
		}
	}

	private Map<Object, Lockers> lockerMap = new ConcurrentHashMap<Object, Lockers>();

	/**
	 * Create and initialize a conservative ordered lock table.
	 */
	public ConservativeLockTable() {
		
	}

	/**
	 * Request lock for an object. This method will put the requested
	 * transaction into a waiting queue of requested object.
	 * 
	 * @param obj
	 *            the object which transaction request lock for
	 * @param txNum
	 *            the transaction that requests the lock
	 */
	synchronized void requestLock(Object obj, long txNum) {
		Lockers lockers = prepareLockers(obj);
		lockers.requestQueue.add(txNum);
	}

	/**
	 * Grants an slock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	synchronized void sLock(Object obj, long txNum) {
		Lockers lockers = prepareLockers(obj);

		// check if it have already held the lock
		if (hasSLock(lockers, txNum)) {
			lockers.requestQueue.remove(txNum);
			return;
		}

		try {
			String name = Thread.currentThread().getName();
			
			/*
			 * If this transaction is not the first one requesting this
			 * object or it cannot get lock on this object, it must wait.
			 */
			Long head = lockers.requestQueue.peek();
			while (!sLockable(lockers, txNum) || (head != null && head.longValue() != txNum)) {

				// For debug
				if (lockers.xLocker != -1) {
					Thread.currentThread().setName(String.format(
							"%s waits for slock of %s from tx.%d (xlock holder)",
							name, obj, lockers.xLocker));
				} else {
					Thread.currentThread().setName(String.format(
							"%s waits for slock of %s from tx.%d (head of queue)",
							name, obj, head));
				}
				
				wait();

				// Since a transaction may delete the lockers of an object
				// after releasing them, it should call prepareLockers()
				// here, instead of using lockers it obtains earlier.
				lockers = prepareLockers(obj);
				head = lockers.requestQueue.peek();
			}

			Thread.currentThread().setName(name);
			
			if (!sLockable(lockers, txNum))
				throw new LockAbortException();

			// get the s lock
			lockers.requestQueue.poll();
			lockers.sLockers.add(txNum);

			// Wake up other waiting transactions (on this object) to let
			// them
			// fight for the lockers on this object.
			notifyAll();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new LockAbortException("Interrupted when waitting for lock");
		}
	}

	/**
	 * Grants an xlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	synchronized void xLock(Object obj, long txNum) {
		Lockers lockers = prepareLockers(obj);

		if (hasXLock(lockers, txNum)) {
			lockers.requestQueue.remove(txNum);
			return;
		}

		try {
			String name = Thread.currentThread().getName();
			
			// long timestamp = System.currentTimeMillis();
			Long head = lockers.requestQueue.peek();
			while ((!xLockable(lockers, txNum) || (head != null && head.longValue() != txNum))
			/* && !waitingTooLong(timestamp) */) {
				
				// For debug
				if (lockers.xLocker != -1) {
					Thread.currentThread().setName(String.format(
							"%s waits for xlock of %s from tx.%d (xlock holder)",
							name, obj, lockers.xLocker));
				} else if (!lockers.sLockers.isEmpty()) {
					Thread.currentThread().setName(String.format(
							"%s waits for xlock of %s from tx.%d (slock holder, %d other holders)",
							name, obj, lockers.sLockers.get(0), lockers.sLockers.size() - 1));
				} else {
					Thread.currentThread().setName(String.format(
							"%s waits for xlock of %s from tx.%d (head of queue)",
							name, obj, head));
				}
				
				wait();
				lockers = prepareLockers(obj);
				head = lockers.requestQueue.peek();
			}

			Thread.currentThread().setName(name);
			
			// if (!xLockable(lockers, txNum))
			// throw new LockAbortException();
			// get the x lock
			lockers.requestQueue.poll();
			lockers.xLocker = txNum;

			// An X lock blocks all other lockers, so it don't need to
			// wake up anyone.
		} catch (InterruptedException e) {
			throw new LockAbortException("Interrupted when waitting for lock");
		}
	}

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * @param obj
	 *            a locked object
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	synchronized void release(Object obj, long txNum, LockType lockType) {
		Lockers lks = lockerMap.get(obj);
		
		if (lks == null)
			return;
		
		releaseLock(lks, txNum, lockType);

		// Remove the locker, if there is no other transaction
		// holding it
		if (!sLocked(lks) && !xLocked(lks)
				&& lks.requestQueue.isEmpty())
			lockerMap.remove(obj);
		
		// There might be someone waiting for the lock
		notifyAll();
	}

	private Lockers prepareLockers(Object obj) {
		Lockers lockers = lockerMap.get(obj);
		if (lockers == null) {
			lockers = new Lockers();
			lockerMap.put(obj, lockers);
		}
		return lockers;
	}

	private void releaseLock(Lockers lks, long txNum, LockType lockType) {
		notifyAll();
		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;
			}
			return;
		case S_LOCK:
			List<Long> sl = lks.sLockers;
			if (sl != null && sl.contains(txNum)) {
				sl.remove((Long) txNum);
			}
			return;
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * Verify if an item is locked.
	 */

	private boolean sLocked(Lockers lks) {
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Lockers lks) {
		return lks != null && lks.xLocker != -1;
	}

	/*
	 * Verify if an item is held by a tx.
	 */

	private boolean hasSLock(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.contains(txNum);
	}

	private boolean hasXLock(Lockers lks, long txNUm) {
		return lks != null && lks.xLocker == txNUm;
	}

	private boolean isTheOnlySLocker(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.size() == 1
				&& lks.sLockers.contains(txNum);
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean sLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean xLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}
}
