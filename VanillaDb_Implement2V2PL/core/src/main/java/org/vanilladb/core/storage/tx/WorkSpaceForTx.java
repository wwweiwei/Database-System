package org.vanilladb.core.storage.tx;

import org.vanilladb.core.storage.*;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import java.util.HashMap;
import java.util.Set;


public class WorkSpaceForTx {
	private HashMap <BlockId,HashMap<Integer,Constant>> privateChange;
	private HashMap <BlockId,LogSeqNum> LsnMap;
	
	public WorkSpaceForTx() {
		this.privateChange = new HashMap <BlockId,HashMap<Integer,Constant>>();
		this.LsnMap = new HashMap <BlockId,LogSeqNum>();
	}
	
	public HashMap <BlockId,HashMap<Integer,Constant>> getP(){
		return this.privateChange;
	}
	
	public HashMap <BlockId, LogSeqNum> getL(){
		return this.LsnMap;
	}
	
	public void addInPrivate(BlockId id, HashMap<Integer,Constant> modifi) {
		this.privateChange.put(id,modifi);
	}
	
	public void saveLsn (BlockId id, LogSeqNum lsn) {
		this.LsnMap.put(id, lsn);
	}

	public boolean containsKeyP(BlockId blk) {
		// TODO Auto-generated method stu
		return this.privateChange.containsKey(blk);
	}
	
	public boolean containsKeyL(BlockId blk) {
		return this.LsnMap.containsKey(blk);
	}
	
	public HashMap<Integer,Constant> getPrivateValue (BlockId blk){
		return this.privateChange.get(blk);
	}
	
	public LogSeqNum getLsnValue (BlockId blk) {
		return this.LsnMap.get(blk);
	}
	
	public void clearPrivate () {
		this.privateChange.clear();
	}
	
	public void clearLsn () {
		this.LsnMap.clear();
	}
}
