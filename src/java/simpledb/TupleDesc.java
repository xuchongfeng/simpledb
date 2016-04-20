package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        
        public Type getFieldType() {
        	return fieldType;
        }
        
        public String getFieldName() {
        	return fieldName;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // return null;
    	return descs.iterator();
    }

    private static final long serialVersionUID = 1L;
    
    /**
     * The collection to store
     */
    private ArrayList<TDItem> descs;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	int typeLength = typeAr.length;
    	int stringLength = fieldAr.length;
    	assert(typeLength == stringLength);
    	
    	descs = new ArrayList<TDItem>();
    	
    	for(int i = 0; i < typeLength; i++) {
    		descs.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	descs = new ArrayList<TDItem>();
    	int length = typeAr.length;
    	for(int i = 0; i < length; i++) {
    		descs.add(new TDItem(typeAr[i], null));
    	}
    }
    
    /**
     * Constructor. Create a new tuple desc with tuple desc, and alias.
     * 
     * @param td
     * @param alias
     */
    public TupleDesc(TupleDesc td, String alias) {
    	int numFields = td.numFields();
    	Type[] typeAr = new Type[numFields];
    	String[] fieldAr = new String[numFields];
    	
    	Iterator<TDItem> iter = td.iterator();
    	int index = 0;
    	while(iter.hasNext()) {
    		TDItem item = iter.next();
    		typeAr[index] = item.getFieldType();
    		fieldAr[index] = alias + "." + item.getFieldName();
    	}
    	for(int i = 0; i < numFields; i++) {
    		descs.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        // return 0;
    	return descs.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        // return null;
    	return descs.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        // return null;
    	return descs.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // return 0;
    	if(name == null) throw new NoSuchElementException();
    	int length = descs.size();
    	for(int i = 0; i < length; i++) {
    		if(name.equals(descs.get(i).fieldName)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // return 0;
    	int size = 0;
    	for(TDItem item: descs) {
    		size += item.fieldType.getLen();
    	}
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        // return null;
    	int numFields1 = td1.numFields();
    	int numFields2 = td2.numFields();
    	int numFields = numFields1 + numFields2;
    	Type[] typeAr = new Type[numFields];
    	String[] fieldAr = new String[numFields];
    	
    	for(int i = 0; i < numFields1; i++) {
    		typeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	
    	for(int i = 0; i < numFields2; i++) {
    		typeAr[i + numFields1] = td2.getFieldType(i);
    		fieldAr[i + numFields1] = td2.getFieldName(i); 
    	}
    	
    	return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        // return false;
    	
    	// check o is null
    	// if(o == null) return false;
    	
    	// check type 
    	if(!(o instanceof TupleDesc)) return false;
    	
    	TupleDesc td = (TupleDesc)o;
    	
    	// compare size
    	if(( this.getSize() != td.getSize())
    		|| (this.numFields() != td.numFields())) return false;
    	
    	// compare field type
    	int numFields = this.numFields();
    	for(int i = 0; i < numFields; i++) {
    		if(this.getFieldType(i) != td.getFieldType(i)) {
    			return false;
    		}
    	}
    	
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
    	return descs.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        // return "";
    	int numFields = descs.size();
    	String[] result = new String[numFields()];
    	for(int i = 0; i < numFields; i++) {
    		String tmp = "";
    		tmp += descs.get(i).fieldType.toString();
    		tmp += "(" + descs.get(i).fieldName.toString() + ")";
    		result[i] = tmp;
    	}
    	return String.join(",", result);
    }
}
