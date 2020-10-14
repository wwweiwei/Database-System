package org.vanilladb.calvin.remote.groupcomm.server;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.cache.InMemoryRecord;
import org.vanilladb.calvin.remote.groupcomm.ClientResponse;
import org.vanilladb.calvin.remote.groupcomm.RecordPackage;
import org.vanilladb.calvin.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.calvin.server.Calvin;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());

	private VanillaCommServer commServer;
	private BlockingQueue<Serializable> tomSendQueue = new LinkedBlockingQueue<Serializable>();
	private boolean areAllServersReady = false;

	public ConnectionMgr(int id) {
		commServer = new VanillaCommServer(id, this);
		new Thread(null, commServer, "VanillaComm-Server").start();
		waitForServersReady();
		createTomSender();
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId,
				new ClientResponse(clientId, rteId, txNum, rs));
	}
	
	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		Object[] spCalls = { new StoredProcedureCall(-1, -1, pid, pars)};
		commServer.sendTotalOrderMessage(spCalls);
	}

	public void pushRecords(int nodeId, RecordPackage records) {
		commServer.sendP2pMessage(ProcessType.SERVER, nodeId, records);
	}
	
	
	//opt
	public void pushRemote(RecordPackage records) {
		//System.out.println("pushRemote");
		commServer.sendTotalOrderMessage(records);
	}
	//opt

	@Override
	public void onServerReady() {
		synchronized (this) {
			areAllServersReady = true;
			this.notifyAll();
		}
	}

	@Override
	public void onServerFailed(int failedServerId) {
		// Do nothing
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.CLIENT) {
			// Normally, the client will only sends its request to the sequencer.
			// However, any other server can also send a total order request.
			// So, we do not need to check if this machine is the sequencer.
			StoredProcedureCall spc = (StoredProcedureCall) message;
			try {
				tomSendQueue.put(spc);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (message.getClass().equals(RecordPackage.class)) {
			RecordPackage pack = (RecordPackage) message;
			for (InMemoryRecord rec : pack.getRecords())
				Calvin.cacheMgr().cacheRemoteRecord(pack.getTxNum(), rec);
		} else
			throw new IllegalArgumentException();
	}

	//opt
	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		if (message.getClass().equals(RecordPackage.class)) {
			RecordPackage pack = (RecordPackage) message;
			for (InMemoryRecord rec : pack.getRecords())
				Calvin.cacheMgr().cacheRemoteRecord(pack.getTxNum(), rec);
		} //Optimization operation: Change to send total 
		else {
			StoredProcedureCall spc = (StoredProcedureCall) message;
			spc.setTxNum(serialNumber);
			Calvin.scheduler().schedule(spc);
		}
	}
	//opt
	
	private void createTomSender() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Serializable messages = tomSendQueue.take();
						commServer.sendTotalOrderMessage(messages);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();;
	}
	
	private void waitForServersReady() {
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		synchronized (this) {
			try {
				while (!areAllServersReady)
					this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
