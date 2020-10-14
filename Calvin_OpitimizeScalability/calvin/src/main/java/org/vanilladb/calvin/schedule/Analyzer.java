package org.vanilladb.calvin.schedule;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.calvin.procedure.ExecutionPlan;
import org.vanilladb.calvin.procedure.ExecutionPlan.ParticipantRole;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.calvin.storage.metadata.PartitionMetaMgr;

public class Analyzer {
	
	private int localNodeId = Calvin.serverId();
	private ExecutionPlan execPlan;
	private PartitionMetaMgr partMgr;
	
	// For read-only transactions to choose one node as a active participant
	private int mostReadsNode;
	private int[] readsPerNodes;
	
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private Set<PrimaryKey> fullyRepReadKeys = new HashSet<PrimaryKey>();
	
	public Analyzer() {
		execPlan = new ExecutionPlan();
		mostReadsNode = 0;
		readsPerNodes = new int[PartitionMetaMgr.NUM_PARTITIONS];
		partMgr = Calvin.partitionMetaMgr();
	}
	
	public ExecutionPlan generatePlan() {
		decideRole();
		
		if (execPlan.getParticipantRole() != ParticipantRole.IGNORE) {
			generatePushSets();
		}
		
		if (execPlan.getParticipantRole() == ParticipantRole.ACTIVE) {
			activePartReadFullyReps();
		}
		
		return execPlan;
	}

	public void addReadKey(PrimaryKey readKey) {
		if (partMgr.isFullyReplicated(readKey)) {
			// We cache it then check if we should add it to the local read set later
			fullyRepReadKeys.add(readKey);
		} else {
			int nodeId = partMgr.getPartition(readKey);
			if (nodeId == localNodeId)
				execPlan.addLocalReadKey(readKey);
			else
				execPlan.addRemoteReadKey(readKey);
			
			// Record who is the node with most readings
			readsPerNodes[nodeId]++;
			if (readsPerNodes[nodeId] > readsPerNodes[mostReadsNode])
				mostReadsNode = nodeId;
		}
	}
	
	public void addUpdateKey(PrimaryKey updateKey) {
		if (partMgr.isFullyReplicated(updateKey)) {
			execPlan.addLocalUpdateKey(updateKey);
		} else {
			int nodeId = partMgr.getPartition(updateKey);
			if (nodeId == localNodeId)
				execPlan.addLocalUpdateKey(updateKey);
			activeParticipants.add(nodeId);
		}
	}
	
	public void addInsertKey(PrimaryKey insertKey) {
		if (partMgr.isFullyReplicated(insertKey)) {
			execPlan.addLocalInsertKey(insertKey);
		} else {
			int nodeId = partMgr.getPartition(insertKey);
			if (nodeId == localNodeId)
				execPlan.addLocalInsertKey(insertKey);
			activeParticipants.add(nodeId);
		}
	}
	
	public void addDeleteKey(PrimaryKey deleteKey) {
		if (partMgr.isFullyReplicated(deleteKey)) {
			execPlan.addLocalDeleteKey(deleteKey);
		} else {
			int nodeId = partMgr.getPartition(deleteKey);
			if (nodeId == localNodeId)
				execPlan.addLocalDeleteKey(deleteKey);
			activeParticipants.add(nodeId);
		}
	}

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private void decideRole() {
		// if there is no active participant (e.g. read-only transaction),
		// choose the one with most readings as the only active participant.
		if (activeParticipants.isEmpty())
			activeParticipants.add(mostReadsNode);
		
		// Decide the role
		if (activeParticipants.contains(localNodeId)) {
			execPlan.setParticipantRole(ParticipantRole.ACTIVE);
		} else if (execPlan.hasLocalReads()) {
			execPlan.setParticipantRole(ParticipantRole.PASSIVE);
		}
	}
	
	private void generatePushSets() {
		for (Integer target : activeParticipants) {
			if (target != localNodeId) {
				for (PrimaryKey key : execPlan.getLocalReadKeys())
					execPlan.addPushSet(target, key);
			}
		}
	}
	
	private void activePartReadFullyReps() {
		for (PrimaryKey key : fullyRepReadKeys)
			execPlan.addLocalReadKey(key);
	}
}
