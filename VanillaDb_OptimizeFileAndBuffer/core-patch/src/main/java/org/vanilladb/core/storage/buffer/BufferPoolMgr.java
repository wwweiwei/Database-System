/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;

import java.util.concurrent.locks.*;
// as4
/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferPoolMgr {
	private static Logger logger = Logger.getLogger(BufferPoolMgr.class.getName());
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private int numAvailable, lastReplacedBuff;
	
	private Lock rL = new ReentrantLock();

	/**
	 * Creates a buffer manager having the specified number of buffer slots. This
	 * constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs the number of buffer slots to allocate
	 */
	BufferPoolMgr(int numBuffs) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Creating " + numBuffs + " buffers");

		bufferPool = new Buffer[numBuffs];
		blockMap = new ConcurrentHashMap<BlockId, Buffer>(numBuffs);
		numAvailable = numBuffs;
		lastReplacedBuff = 0;
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();

		if (logger.isLoggable(Level.INFO))
			logger.info("Buffer pool is ready (assignment 4 version)");
	}

	/**
	 * Flushes all dirty buffers.
	 */
	void flushAll() {
		rL.lock();
		try {
			for (Buffer buff : bufferPool)
				buff.flush();
		} finally {
			rL.unlock();
		}
	} // as4

	/**
	 * Pins a buffer to the specified block. If there is already a buffer assigned
	 * to that block then that buffer is used; otherwise, an unpinned buffer from
	 * the pool is chosen. Returns a null value if there are no available buffers.
	 * 
	 * @param blk a block ID
	 * @return the pinned buffer
	 */
	Buffer pin(BlockId blk) {
		rL.lock();
		try {
			Buffer buff = findExistingBuffer(blk);
			if (buff == null) {
				buff = chooseUnpinnedBuffer();
				if (buff == null)
					return null;
				BlockId oldBlk = buff.block();
				if (oldBlk != null)
					blockMap.remove(oldBlk);
				buff.assignToBlock(blk);
				blockMap.put(blk, buff);
			}
			if (!buff.isPinned())
				numAvailable--;
			buff.pin();
			return buff;
		} finally {
			rL.unlock();
		}
	} // as4

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it. Returns
	 * null (without allocating the block) if there are no available buffers.
	 * 
	 * @param fileName the name of the file
	 * @param fmtr     a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	Buffer pinNew(String fileName, PageFormatter fmtr) {
		rL.lock();
		try {
			Buffer buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			BlockId oldBlk = buff.block();
			if (oldBlk != null)
				blockMap.remove(oldBlk);

			buff.assignToNew(fileName, fmtr);
			numAvailable--;
			buff.pin();
			blockMap.put(buff.block(), buff);
			return buff;
		} finally {
			rL.unlock();
		}
	} // as4

	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		rL.lock();
		try {
			for (Buffer buff : buffs) {
				buff.unpin();
				if (!buff.isPinned())
					numAvailable++;
			}
		} finally {
			rL.unlock();
		} // as4

	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		rL.lock();
		try {
			return numAvailable;
		} finally {
			rL.unlock();
		}
	} // as4

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = blockMap.get(blk);
		if (buff != null && buff.block().equals(blk))
			return buff;
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
		while (currBlk != lastReplacedBuff) {
			Buffer buff = bufferPool[currBlk];
			if (!buff.isPinned()) {
				lastReplacedBuff = currBlk;
				return buff;
			}
			currBlk = (currBlk + 1) % bufferPool.length;
		}
		return null;
	}
}
