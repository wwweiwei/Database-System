package org.vanilladb.calvin.storage.log;

import org.vanilladb.calvin.util.CalvinProperties;
import org.vanilladb.core.storage.log.LogMgr;

public class CalvinLogMgr extends LogMgr {

	public static final String DD_LOG_FILE;

	static {
		DD_LOG_FILE = CalvinProperties.getLoader().getPropertyAsString(
				CalvinLogMgr.class.getName() + ".LOG_FILE", "calvin.log");
	}

	public CalvinLogMgr() {
		super(DD_LOG_FILE);
	}
}
