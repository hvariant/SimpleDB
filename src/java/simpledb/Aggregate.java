package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public int afield;
    public int gfield;
    public Aggregator.Op aop = null;

    public DbIterator child = null;

    public Aggregator ag = null;
    public DbIterator ag_it = null;
    //@ADDED

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) { //@ADDED
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gtyp = null;
        if(gfield != Aggregator.NO_GROUPING)
            gtyp = child.getTupleDesc().getFieldType(gfield);

        Type typ = child.getTupleDesc().getFieldType(afield);

        if(typ == null){
            throw new IllegalArgumentException();
        } else if(typ == Type.INT_TYPE){ //@TODO: reference or value?
            this.ag = new IntegerAggregator(gfield,gtyp,afield,aop);
        } else if(typ == Type.STRING_TYPE){
            this.ag = new StringAggregator(gfield,gtyp,afield,aop);
        } else {
            throw new IllegalArgumentException();
        }

        this.child = child;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() { //@ADDED
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() { //@ADDED
        if(gfield == Aggregator.NO_GROUPING){
            return null;
        }

        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() { //@ADDED
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() { //@ADDED
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() { //@ADDED
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException { //@ADDED
        super.open();

        if(ag_it == null){
            child.open();

            while(child.hasNext()){
                Tuple tp = child.next();
                ag.mergeTupleIntoGroup(tp);
            }

            ag_it = ag.iterator();
        }

        ag_it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //@ADDED
        if(!ag_it.hasNext()) return null;

        return ag_it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException { //@ADDED
        ag_it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() { //@ADDED
        return child.getTupleDesc();
    }

    public void close() { //@ADDED
        super.close();
        ag_it.close();
    }

    @Override
    public DbIterator[] getChildren() { //@ADDED
        DbIterator[] tmp = new DbIterator[1];
        tmp[0] = this.ag_it;

        return tmp;
    }

    @Override
    public void setChildren(DbIterator[] children) { //@ADDED
        assert children.length == 1;
    }
    
}
