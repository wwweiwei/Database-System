package org.vanilladb.calvin.procedure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vanilladb.calvin.sql.PrimaryKey;

public class ExecutionPlan {
	
	public enum ParticipantRole { ACTIVE, PASSIVE, IGNORE };
	
	public static class PushSet {
		Set<PrimaryKey> keys;
		Set<Integer> nodeIds;
		
		public PushSet(Set<PrimaryKey> keys, Set<Integer> nodeIds) {
			this.keys = keys;
			this.nodeIds = nodeIds;
		}
		
		public Set<PrimaryKey> getPushKeys() {
			return keys;
		}
		
		public Set<Integer> getPushNodeIds() {
			return nodeIds;
		}
		
		@Override
		public String toString() {
			return "Keys: " + keys + ", targets: " + nodeIds;
		}
	}
	
	private ParticipantRole role = ParticipantRole.IGNORE;
	
	// Record keys for normal operations
	private Set<PrimaryKey> localReadKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> remoteReadKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> localUpdateKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> localInsertKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> localDeleteKeys = new HashSet<PrimaryKey>();
	private Map<Integer, Set<PrimaryKey>> pushSets = new HashMap<Integer, Set<PrimaryKey>>();
	
	private boolean forceReadWriteTx = false;
	private boolean forceRemoteReadEnabled = false;

	public void addLocalReadKey(PrimaryKey key) {
		localReadKeys.add(key);
	}
	
	public void addRemoteReadKey(PrimaryKey key) {
		remoteReadKeys.add(key);
	}
	
	public void addLocalUpdateKey(PrimaryKey key) {
		localUpdateKeys.add(key);
	}

	public void addLocalInsertKey(PrimaryKey key) {
		localInsertKeys.add(key);
	}

	public void addLocalDeleteKey(PrimaryKey key) {
		localDeleteKeys.add(key);
	}
	
	public void addPushSet(Integer targetNodeId, PrimaryKey key) {
		Set<PrimaryKey> keys = pushSets.get(targetNodeId);
		if (keys == null) {
			keys = new HashSet<PrimaryKey>();
			pushSets.put(targetNodeId, keys);
		}
		keys.add(key);
	}
	
	public void removeFromPushSet(Integer targetNodeId, PrimaryKey key) {
		Set<PrimaryKey> keys = pushSets.get(targetNodeId);
		if (keys != null) {
			keys.remove(key);
			
			if (keys.size() == 0)
				pushSets.remove(targetNodeId);
		}
	}
	
	public void setForceReadWriteTx() {
		forceReadWriteTx = true;
	}
	
	public void setRemoteReadEnabled() {
		forceRemoteReadEnabled = true;
	}
	
	public Set<PrimaryKey> getLocalReadKeys() {
		return localReadKeys;
	}
	
	public Set<PrimaryKey> getRemoteReadKeys() {
		return remoteReadKeys;
	}
	
	public boolean isLocalUpdate(PrimaryKey key) {
		return localUpdateKeys.contains(key);
	}
	
	public Set<PrimaryKey> getLocalUpdateKeys() {
		return localUpdateKeys;
	}
	
	public boolean isLocalInsert(PrimaryKey key) {
		return localInsertKeys.contains(key);
	}
	
	public Set<PrimaryKey> getLocalInsertKeys() {
		return localInsertKeys;
	}
	
	public boolean isLocalDelete(PrimaryKey key) {
		return localDeleteKeys.contains(key);
	}
	
	public Set<PrimaryKey> getLocalDeleteKeys() {
		return localDeleteKeys;
	}
	
	public Map<Integer, Set<PrimaryKey>> getPushSets() {
		return pushSets;
	}
	
	public void setParticipantRole(ParticipantRole role) {
		this.role = role;
	}
	
	public ParticipantRole getParticipantRole() {
		return role;
	}
	
	public boolean isReadOnly() {
		if (forceReadWriteTx)
			return false;
		
		return localUpdateKeys.isEmpty() && localInsertKeys.isEmpty() &&
				localDeleteKeys.isEmpty();
	}
	
	public boolean hasLocalReads() {
		return !localReadKeys.isEmpty();
	}
	
	public boolean hasRemoteReads() {
		if (forceRemoteReadEnabled)
			return true;
		
		return !remoteReadKeys.isEmpty();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("============== Execution Plan ==============");
		sb.append('\n');
		sb.append("Role: " + role);
		sb.append('\n');
		sb.append("Local Reads: " + localReadKeys);
		sb.append('\n');
		sb.append("Remote Reads: " + remoteReadKeys);
		sb.append('\n');
		sb.append("Local Updates: " + localUpdateKeys);
		sb.append('\n');
		sb.append("Local Inserts: " + localInsertKeys);
		sb.append('\n');
		sb.append("Local Deletes: " + localDeleteKeys);
		sb.append('\n');
		sb.append("Push Sets: " + pushSets);
		sb.append('\n');
		sb.append("===========================================");
		sb.append('\n');
		
		return sb.toString();
	}
}
