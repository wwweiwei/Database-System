package org.vanilladb.calvin.procedure;

public interface CalvinStoredProcedureFactory {
	
	CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum);
	
}
