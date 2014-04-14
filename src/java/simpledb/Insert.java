package simpledb;

import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public TransactionId tid = null;
    public DbIterator child = null;
    public int tableid;

    public boolean insertDone = false;
    public Tuple ret = null;

    public DbIterator[] children = null;
    //@ADDED

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException { //@ADDED
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        
        this.insertDone = false;

        this.children = new DbIterator[1];
        this.children[0] = child;

        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;

        this.ret = new Tuple(new TupleDesc(typeAr));
    }

    public TupleDesc getTupleDesc() { //@ADDED
        return ret.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException { //@ADDED
        super.open();
        child.open();
    }

    public void close() { //@ADDED
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException { //@ADDED
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //@ADDED
        if(insertDone) return null;
        BufferPool pool = Database.getBufferPool();
        int tps = 0;

        while(child.hasNext()){
            Tuple t = child.next();
            try{
                pool.insertTuple(tid,tableid,t);
                tps++;
            } catch(IOException e) {
                throw new DbException("error inserting tuple:" + e);
            }
        }

        ret.setField(0,new IntField(tps));
        insertDone = true;

        return ret;
    }

    @Override
    public DbIterator[] getChildren() { //@ADDED
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) { //@ADDED
        assert children.length == 1;

        child = children[0];
        this.children[0] = child;
    }
}
