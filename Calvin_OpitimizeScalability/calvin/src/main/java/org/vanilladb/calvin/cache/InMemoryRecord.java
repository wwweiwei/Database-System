package org.vanilladb.calvin.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vanilladb.calvin.sql.FieldNotFoundException;
import org.vanilladb.calvin.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

public class InMemoryRecord implements Record, Serializable {

	private static final long serialVersionUID = 20200107002L;

	private boolean isDirty, isDeleted, isNewInserted;
	private boolean isTemp; // the temporary record will not be flushed.
	
	private PrimaryKey primaryKey;
	// A Constant is non-serializable
	private transient Map<String, Constant> nonKeyFldVals = 
			new HashMap<String, Constant>();
	private List<String> dirtyFlds = new ArrayList<String>();
	
	private int hashCode = 17; //opt

	public static InMemoryRecord newRecordWithFldVals(PrimaryKey key,
			Map<String, Constant> fldVals) {
		InMemoryRecord rec = new InMemoryRecord(key);
		for (Map.Entry<String, Constant> entry : fldVals.entrySet()) {
			String fld = entry.getKey();
			Constant val = entry.getValue();
			rec.addFldVal(fld, val);
		}
		return rec;
	}
	
	public static InMemoryRecord newRecordForInsertion(PrimaryKey key,
			Map<String, Constant> fldVals) {
		InMemoryRecord rec = newRecordWithFldVals(key, fldVals);
		rec.isNewInserted = true;
		rec.isDirty = true;
		
		rec.hashCode = 31 * rec.hashCode + key.hashCode(); // opt
		rec.hashCode = 31 * rec.hashCode + fldVals.hashCode(); // opt
		
		return rec;
	}
	
	public static InMemoryRecord newRecordForDeletion(PrimaryKey key) {
		InMemoryRecord rec = new InMemoryRecord(key);
		rec.isDeleted = true;
		rec.isDirty = true;
		
		rec.hashCode = 31 * rec.hashCode + key.hashCode(); // opt
		
		return rec;
	}
	
	public InMemoryRecord(PrimaryKey primaryKey) {
		this.primaryKey = primaryKey;
		
		this.hashCode = 31 * this.hashCode + this.hashCode(); // opt
		
	}
	
	/**
	 * Constructs a new InMemoryRecord with the same key-value pairs
	 * and the same meta-data as the given InMemoryRecord.
	 */
	public InMemoryRecord(InMemoryRecord rec) {
		primaryKey = rec.primaryKey;
		nonKeyFldVals = new HashMap<String, Constant>(rec.nonKeyFldVals);
		dirtyFlds = new ArrayList<String>(rec.dirtyFlds);
		isDirty = rec.isDirty;
		isDeleted = rec.isDeleted;
		isNewInserted = rec.isNewInserted;
		isTemp = rec.isTemp;
	}

	public Constant getVal(String fldName) {
		if (isDeleted) {
			return null;
		} else {
			// Check the key first
			Constant val = primaryKey.getVal(fldName);
			if (val != null)
				return val;
			
			// Check the map
			return nonKeyFldVals.get(fldName);
		}
	}
	
	public PrimaryKey getPrimaryKey() {
		return primaryKey;
	}
	
	public void addFldVal(String field, Constant val) {
		Constant keyVal = primaryKey.getVal(field);
		if (keyVal == null)
			nonKeyFldVals.put(field, val);
		else if (!keyVal.equals(val))
			throw new UnsupportedOperationException(
					"cannot modify key field: " + field);
	}
	
	public Constant removeField(String field) {
		if (primaryKey.containsField(field))
			throw new UnsupportedOperationException(
					"cannot remove key field: " + field);
		Constant val = nonKeyFldVals.remove(field);
		if (val != null)
			dirtyFlds.remove(field);
		return val;
	}

	public void setVal(String fldName, Constant val) {
		if (isDeleted)
			throw new UnsupportedOperationException("the record " +
					primaryKey + " is deleted.");
		
		if (primaryKey.containsField(fldName))
			throw new UnsupportedOperationException("cannot modify key field: " + fldName);
		
		if (!nonKeyFldVals.containsKey(fldName))
			throw new FieldNotFoundException(fldName);
		
		isDirty = true;
		if (!dirtyFlds.contains(fldName))
			dirtyFlds.add(fldName);
		
		nonKeyFldVals.put(fldName, val);
	}
	
	public void markAllNonKeyFieldsDirty() {
		dirtyFlds.clear();
		dirtyFlds.addAll(nonKeyFldVals.keySet());
	}

	public boolean isDirty() {
		return isDirty;
	}

	public boolean isDeleted() {
		return isDeleted;
	}

	public boolean isNewInserted() {
		return isNewInserted;
	}
	
	public void delete() {
		isDeleted = true;
		isDirty = true;
	}
	
	public void setNewInserted() {
		isNewInserted = true;
		isDirty = true;
	}
	
	public void setTempRecord(boolean isTemp) {
		this.isTemp = isTemp;
	}
	
	public List<String> getFldNames() {
		List<String> allFields = 
				new ArrayList<String>(nonKeyFldVals.keySet());
		for (int i = 0; i < primaryKey.getNumOfFlds(); i++)
			allFields.add(primaryKey.getField(i));
		return allFields;
	}
	
	public List<String> getDirtyFldNames() {
		return new ArrayList<String>(dirtyFlds);
	}
	
	public Map<String, Constant> toFldValMap() {
		Map<String, Constant> fldVals =
				new HashMap<String, Constant>(nonKeyFldVals);
		for (int i = 0; i < primaryKey.getNumOfFlds(); i++)
			fldVals.put(primaryKey.getField(i), primaryKey.getVal(i));
		return fldVals;
	}

	public boolean isTemp() {
		return isTemp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(primaryKey.getTableName());
		sb.append(": {");
		
		// Key fields
		for (int i = 0; i < primaryKey.getNumOfFlds(); i++) {
			sb.append("*");
			sb.append(primaryKey.getField(i));
			sb.append(": ");
			sb.append(primaryKey.getVal(i));
			sb.append(", ");
		}
		
		// Other fields
		for (Map.Entry<String, Constant> entry : nonKeyFldVals.entrySet()) {
			sb.append(entry.getKey());
			sb.append(": ");
			sb.append(entry.getValue());
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}");
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof InMemoryRecord))
			return false;
		InMemoryRecord rec = (InMemoryRecord) obj;
		return rec.primaryKey.equals(this.primaryKey) &&
				rec.nonKeyFldVals.equals(this.nonKeyFldVals);
	}

	@Override
	public int hashCode() {
		
		return hashCode; // opt
		
		// int hashCode = 17;
		// hashCode = 31 * hashCode + primaryKey.hashCode();
		// hashCode = 31 * hashCode + nonKeyFldVals.hashCode();
		// return hashCode;
	}

	/**
	 * Serialize this {@code CachedRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(nonKeyFldVals.size());

		// Write out all elements in the proper order
		for (Map.Entry<String, Constant> entry : nonKeyFldVals.entrySet()) {
			String fld = entry.getKey();
			Constant val = entry.getValue();
			byte[] bytes = val.asBytes();
			out.writeObject(fld);
			out.writeInt(val.getType().getSqlType());
			out.writeInt(val.getType().getArgument());
			out.writeInt(bytes.length);
			out.write(bytes);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		int numFlds = in.readInt();
		nonKeyFldVals = new HashMap<String, Constant>(numFlds);

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			int argument = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(
				Type.newInstance(sqlType, argument),
				bytes
			);
			nonKeyFldVals.put(fld, val);
		}
	}
}
