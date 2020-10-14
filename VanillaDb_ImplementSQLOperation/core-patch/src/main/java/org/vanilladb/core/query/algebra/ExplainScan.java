// as3
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

/**
 * The scan class corresponding to the <em>explain</em> relational algebra
 * operator.
 */
public class ExplainScan implements Scan {

	private Scan s;
	private int actualNum;
	private boolean first;
	private String planInfo;

	public ExplainScan(Scan s, String planInfo) {
		first=true;
		this.s = s;
		this.planInfo = planInfo;
		s.beforeFirst();
		while (s.next())
			actualNum++;
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan")) {
			return new VarcharConstant("\n" + planInfo + "Actual #recs: " + actualNum + "\n");
		} else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		if (first) {
			first = false;
			return true;
		} else
			return false;
	}

	@Override
	public void close() {
		
	}

	@Override
	public boolean hasField(String fldname) {
		if (fldname.equals("query-plan"))
			return true;
		else
			return false;

	}
}

