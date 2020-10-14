package org.vanilladb.calvin.util;

import org.vanilladb.core.util.PropertiesLoader;

public class CalvinProperties extends PropertiesLoader {

	private static CalvinProperties loader;
	
	public static CalvinProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new CalvinProperties();
		return loader;
	}
	
	protected CalvinProperties() {
		super();
	}
	
	@Override
	protected String getConfigFilePath() {
		return System.getProperty("org.vanilladb.calvin.config.file");
	}

}
