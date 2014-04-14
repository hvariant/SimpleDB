package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    public Predicate p = null;
    public DbIterator child = null;
    public DbIterator[] children = null;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) { //@ADDED
        this.p = p;
        this.child = child;

        children = new DbIterator[1];
        children[0] = child;
    }

    public Predicate getPredicate() { //@ADDED
        return p;
    }

    public TupleDesc getTupleDesc() { //@ADDED
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException { //@ADDED
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException { //@ADDED
        while(child.hasNext()){
            Tuple t = child.next();

            if(p.filter(t)){
                return t;
            }
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() { //@TODO:? why array
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) { //@TODO:? why array
        assert children.length == 1;
        child = children[0];

        this.children[0] = child;
    }

}
