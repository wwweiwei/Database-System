// as3
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan{
	
	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;

	public ExplainPlan(Plan p) {
		System.out.printf("expalin-plan"+"\n");
		this.p = p;
		// this.hist = p.histogram();
		schema.addField("query-plan", Type.VARCHAR(500));
	}
	
	@Override
	public Scan open() {
		System.out.printf("scan-open"+"\n");
		return new ExplainScan(p.open(),getPlanInfo(""));
	}
	
	public String getPlanInfo(String t) {
		return p.getPlanInfo(t);
	}

	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

}