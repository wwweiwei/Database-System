package org.vanilladb.calvin.remote.groupcomm.client;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.calvin.remote.groupcomm.ClientResponse;
import org.vanilladb.calvin.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.calvin.storage.metadata.PartitionMetaMgr;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.client.VanillaCommClientListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class GroupCommConnection implements VanillaCommClientListener {
	
	// This server should be the leader of Zab
	private static final int TARGET_SERVER_ID = PartitionMetaMgr.NUM_PARTITIONS - 1;
	
	// RTE id -> A blocking queue of responses from servers
	private Map<Integer, BlockingQueue<ClientResponse>> rteToRespQueue = new ConcurrentHashMap<Integer, BlockingQueue<ClientResponse>>();
	// RTE id -> The transaction number of the received response last time
	private Map<Integer, Long> rteToLastTxNum = new ConcurrentHashMap<Integer, Long>();
	
	private VanillaCommClient commClient;
	private int myId;
	private DirectMessageListener directMessageListener;

	public GroupCommConnection(int id, DirectMessageListener directMessageListener) {
		this.myId = id;
		this.directMessageListener = directMessageListener;

		// Initialize group communication
		commClient = new VanillaCommClient(id, this);
		
		// Create a thread for it
		new Thread(null, commClient, "VanillaComm-Clinet").start();
	}

	public SpResultSet callStoredProc(int connId, int pid, Object... pars) {
		// Check if there is a queue for it
		BlockingQueue<ClientResponse> respQueue = rteToRespQueue.get(connId);
		if (respQueue == null) {
			respQueue = new LinkedBlockingQueue<ClientResponse>();
			rteToRespQueue.put(connId, respQueue);
		}
		
		// Send the parameters as a stored procedure call
		StoredProcedureCall spc = new StoredProcedureCall(myId, connId, pid, pars);
		commClient.sendP2pMessage(ProcessType.SERVER, TARGET_SERVER_ID, spc);

		// Wait for the response
		try {
			ClientResponse cr = respQueue.take();
			Long lastTxNumObj = rteToLastTxNum.get(connId);
			long lastTxNum = -1;
			if (lastTxNumObj != null)
				lastTxNum = lastTxNumObj;

			while (lastTxNum >= cr.getTxNum())
				cr = respQueue.take();

			// Record the tx number of the response
			rteToLastTxNum.put(connId, cr.getTxNum());
			
			return cr.getResultSet();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException("Something wrong");
		}
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.SERVER) {
			ClientResponse c = (ClientResponse) message;
			
			// Check if this response is for this node
			if (c.getClientId() == myId) {
				rteToRespQueue.get(c.getRteId()).add(c);
			} else {
				throw new RuntimeException("Something wrong");
			}
		} else {
			directMessageListener.onReceivedDirectMessage(message);
		}
	}
	
	public void sendP2pMessageToClientNode(int clientId, Serializable message) {
		commClient.sendP2pMessage(ProcessType.CLIENT, clientId, message);
	}
	
	public int getServerCount() {
		return commClient.getServerCount();
	}
	
	public int getClientCount() {
		return commClient.getClientCount();
	}
}
