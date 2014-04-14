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

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() { //@ADDED
        return Arrays.asList(items).iterator();
    }

    private static final long serialVersionUID = 1L;
    public TDItem[] items = null; //@ADDED

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
    public TupleDesc(Type[] typeAr, String[] fieldAr) { //@ADDED
        int L = typeAr.length;
        assert fieldAr.length == L;

        items = new TDItem[L];

        for(int i=0;i<L;i++){
            items[i] = new TDItem(typeAr[i],fieldAr[i]);
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
    public TupleDesc(Type[] typeAr) { //@ADDED
        int L = typeAr.length;

        items = new TDItem[L];
        for(int i=0;i<L;i++){
            items[i] = new TDItem(typeAr[i],"");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() { //@ADDED
        return items.length;
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
    public String getFieldName(int i) throws NoSuchElementException { //@ADDED
        if(i < 0 || i >= items.length){
            throw new NoSuchElementException();
        }

        return items[i].fieldName;
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
    public Type getFieldType(int i) throws NoSuchElementException { //@ADDED
        if(i < 0 || i >= items.length){
            throw new NoSuchElementException();
        }

        return items[i].fieldType;
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
    public int fieldNameToIndex(String name) throws NoSuchElementException { //@ADDED
        if(name == null){
            throw new NoSuchElementException();
        }

        for(int i=0;i<items.length;i++){
            String field = null;

            int j;
            for(j=name.length()-1;j>=0;j--){
                if(name.charAt(j) == '.') break;
            }
            if(j >= 0){
                field = name.substring(j+1);
            } else {
                field = name;
            }

            //System.out.println(items[i].fieldName + " =? " + field);
            if(items[i].fieldName.equals(field)){
                return i;
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() { //@ADDED
        int size = 0;
        for(int i=0;i<items.length;i++){
            size += items[i].fieldType.getLen();
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
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) { //@ADDED
        int l1 = td1.numFields();
        int l2 = td2.numFields();
        int L = l1+l2;

        Type[] typeAr = new Type[L];
        String[] nameAr = new String[L];

        for(int i=0;i<l1;i++){
            typeAr[i] = td1.getFieldType(i);
            nameAr[i] = td1.getFieldName(i);
        }
        for(int i=0;i<l2;i++){
            typeAr[i+l1] = td2.getFieldType(i);
            nameAr[i+l1] = td2.getFieldName(i);
        }

        return new TupleDesc(typeAr,nameAr);
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
    public boolean equals(Object o) { //@ADDED
        if(o == null) return false;

        if(o.getClass() != TupleDesc.class){
            return false;
        }
        TupleDesc td = (TupleDesc)o;

        if(td.getSize() != getSize() || td.numFields() != numFields()){
            return false;
        }

        for(int i=0;i<td.numFields();i++){
            if(td.getFieldType(i).getLen() != this.getFieldType(i).getLen())
                return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() { //@ADDED
        StringBuffer b = new StringBuffer("");

        b.append(getFieldType(0).toString() + "(" + getFieldName(0) + ")");
        for(int i=1;i<numFields();i++){
            b.append("," + getFieldType(i).toString() + "(" + getFieldName(i) + ")");
        }

        return b.toString();
    }
}
