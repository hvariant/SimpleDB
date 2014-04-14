package simpledb;

import java.io.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public TransactionId tid = null;
    public DbIterator child = null;

    public boolean deleteDone = false;
    public Tuple ret = null;

    public DbIterator[] children = null;
    //@ADDED

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) { //@ADDED
        this.tid = t;
        this.child = child;
        
        this.deleteDone = false;

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //@ADDED
        if(deleteDone) return null;

        BufferPool pool = Database.getBufferPool();
        int tps = 0;

        while(child.hasNext()){
            Tuple t = child.next();
            pool.deleteTuple(tid,t);
            tps++;
        }

        ret.setField(0,new IntField(tps));
        deleteDone = true;

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
