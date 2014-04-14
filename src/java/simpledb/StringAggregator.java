package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public Op op = null;
    public int gbfield;
    public int afield;
    public Type gbfieldtype = null;

    public TupleDesc td;

    public int result;
    public Map<Field,Integer> gb_results = null;
    //@ADDED


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) { //@ADDED
        if(what != Op.COUNT){ //@TODO: reference or value?
            throw new IllegalArgumentException();
        }

        this.op = what;
        this.afield = afield; //not needed?
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;

        Type[] typeAr = null;
        if(this.gbfield == NO_GROUPING){
            typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
        } else {
            typeAr = new Type[2];
            typeAr[0] = gbfieldtype;
            typeAr[1] = Type.INT_TYPE;
        }
        this.td = new TupleDesc(typeAr);

        if(this.gbfield == NO_GROUPING){
            this.result = 0;
        } else {
            this.gb_results = new HashMap<Field,Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) { //@ADDED
        if(this.gbfield == NO_GROUPING){
            this.result++;

            return;
        }

        Field tup_gf = tup.getField(gbfield);
        
        assert tup_gf.getType() == gbfieldtype;

        if(this.gb_results.get(tup_gf) == null){
            this.gb_results.put(tup_gf,1);
        } else {
            Integer i = (Integer)this.gb_results.get(tup_gf);
            this.gb_results.put(tup_gf, new Integer(i.intValue() + 1));

        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() { //@ADDED
        if(gbfield == NO_GROUPING){
            return new DbIterator() { //@ADDED
                public boolean opened = false;
                public boolean closed = false;
                public boolean done = false;
                public Tuple r = null;

                public void open()
                    throws DbException, TransactionAbortedException{
                    if(opened) return;
                    opened = true;

                    this.r = new Tuple(StringAggregator.this.td);
                    this.r.setField(0,new IntField(StringAggregator.this.result));
                }

                public boolean hasNext() throws DbException, TransactionAbortedException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");

                    return !done;
                }

                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");
                    if(done) throw new NoSuchElementException();

                    done = true;
                    return r;
                }

                public void rewind() throws DbException, TransactionAbortedException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");

                    done = false;
                }

                public TupleDesc getTupleDesc(){
                    return StringAggregator.this.td;
                }

                public void close(){
                    closed = true;
                }
            };
        } else {
            return new DbIterator() { //@ADDED
                public boolean opened = false;
                public boolean closed = false;
                public boolean done = false;
                public Iterator iter = null;

                public void open()
                    throws DbException, TransactionAbortedException{
                    if(opened) return;
                    opened = true;

                    iter = StringAggregator.this.gb_results.entrySet().iterator();
                }

                public boolean hasNext() throws DbException, TransactionAbortedException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");

                    return iter.hasNext();
                }

                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");
                    if(!iter.hasNext()) throw new NoSuchElementException();

                    Map.Entry entry = (Map.Entry) iter.next();
                    Field gf = (Field)entry.getKey();
                    Integer i = (Integer)entry.getValue();

                    Tuple r = new Tuple(StringAggregator.this.td);
                    r.setField(0,gf);
                    r.setField(1,new IntField(i.intValue()));

                    return r;
                }

                public void rewind() throws DbException, TransactionAbortedException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");

                    iter = StringAggregator.this.gb_results.entrySet().iterator();
                }

                public TupleDesc getTupleDesc(){
                    return StringAggregator.this.td;
                }

                public void close(){
                    closed = true;
                    iter = null;
                }
            };
            
        }
    }

}
