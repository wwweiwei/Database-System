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
package org.vanilladb.core.remote.storedprocedure;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteDriver.
 */
@SuppressWarnings("serial")
public class RemoteDriverImpl extends UnicastRemoteObject implements
		RemoteDriver {

	public RemoteDriverImpl() throws RemoteException {
	}

	/**
	 * Creates a new RemoteConnectionImpl object and returns it.
	 * 
	 * @see RemoteDriver#connect()
	 */
	@Override
	public RemoteConnection connect() throws RemoteException {
		return new RemoteConnectionImpl();
	}
}
