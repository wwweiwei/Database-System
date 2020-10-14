package org.vanilladb.calvin.remote.groupcomm.client;

public class GroupCommDriver {

	private int myId;

	public GroupCommDriver(int id) {
		myId = id;
	}

	public GroupCommConnection connect(DirectMessageListener messageListener) {
		GroupCommConnection bc = new GroupCommConnection(myId, messageListener);
		return bc;
	}
}
