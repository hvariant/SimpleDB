package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public TupleDesc tupledesc = null;
    public Field[] fields = null;
    public RecordId rid = null;
    //@ADDED

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) { //@ADDED
        assert(td != null);
        assert(td.numFields() > 0);

        tupledesc = td;
        fields = new Field[tupledesc.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() { //@ADDED
        return tupledesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() { //@ADDED
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) { //@ADDED
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) { //@ADDED
        assert 0 <= i && i < fields.length;
        assert tupledesc.getFieldType(i).getLen() == f.getType().getLen();
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) { //@ADDED
        assert 0 <= i && i < fields.length;
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() { //@ADDED
        StringBuffer b = new StringBuffer("");

        b.append(this.getField(0).toString());
        for(int i=1;i<fields.length;i++){
            b.append(" " + this.getField(i).toString());
        }

        return b.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() //@ADDED
    {
        return Arrays.asList(fields).iterator();
    }
}
