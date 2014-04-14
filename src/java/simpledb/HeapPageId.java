package simpledb;

import java.io.*;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */

    public int tableId;
    public int pageNumber;

    public HeapPageId(int tableId, int pgNo) { //@ADDED
        this.tableId = tableId;
        this.pageNumber = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() { //@ADDED
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() { //@ADDED
        return pageNumber;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() { //@ADDED
        return tableId << 16 + pageNumber % (1<<16);  //@TODO: good hash?
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) { //@ADDED
        if(o == null) return false;
        if(o.getClass() != HeapPageId.class) return false;

        return ((HeapPageId)o).getTableId() == getTableId() && ((HeapPageId)o).pageNumber() == pageNumber();
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }
}
