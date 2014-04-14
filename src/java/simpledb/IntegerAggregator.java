package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //@ADDED
    public class Stats{
        public int sum = 0;
        public int max = Integer.MIN_VALUE;
        public int min = Integer.MAX_VALUE;
        public int count = 0;

        public Stats(){}

        public void addValue(int i){
            sum += i;
            count++;
            if(i > max) max = i;
            if(i < min) min = i;
        }

        public int avg(){
            if(count == 0) return 0; //@hack?
            return sum/count;
        }
        
        public String toString(){
            return "sum=" + sum + ";" + "count=" + count + ";max=" + max + ";min=" + min
                    + ";avg=" + avg();
        }
    }

    public Op op = null;
    public int gbfield;
    public int afield;
    public Type gbfieldtype = null;

    public TupleDesc td;

    public Stats result = null;
    public Map<Field,Stats> gb_results = null;
    //@ADDED

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) { //@ADDED
        this.op = what;
        this.afield = afield;
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
            this.result = new Stats();
        } else {
            this.gb_results = new HashMap<Field,Stats>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) { //@ADDED
        IntField tup_af = (IntField)tup.getField(afield);
        int val = tup_af.getValue();

        if(this.gbfield == NO_GROUPING){
            this.result.addValue(val);

            return;
        }

        Field tup_gf = tup.getField(gbfield);

        if(this.gb_results.get(tup_gf) == null){
            Stats s = new Stats();
            s.addValue(val);

            this.gb_results.put(tup_gf,s);
        } else {
            Stats s = (Stats)this.gb_results.get(tup_gf);
            s.addValue(val);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        if(gbfield == NO_GROUPING){
            return new DbIterator() { //@ADDED
                public boolean opened = false;
                public boolean closed = false;
                public boolean done = false;
                public Tuple r = null;

                //MIN, MAX, SUM, AVG, COUNT;
                public void fillValue(Stats s,Tuple r){
                    Op op = IntegerAggregator.this.op;
                    if(op == Op.MIN){
                        r.setField(0,new IntField(s.min));
                    } else if(op == Op.MAX){
                        r.setField(0,new IntField(s.max));
                    } else if(op == Op.SUM){
                        r.setField(0,new IntField(s.sum));
                    } else if(op == Op.AVG){
                        r.setField(0,new IntField(s.avg()));
                    } else {
                        r.setField(0,new IntField(s.count));
                    }
                }

                public void open()
                    throws DbException, TransactionAbortedException{
                    if(opened) return;
                    opened = true;

                    this.r = new Tuple(IntegerAggregator.this.td);
                    fillValue(IntegerAggregator.this.result,this.r);
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
                    return IntegerAggregator.this.td;
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

                public void fillValue(Field f,Stats s,Tuple r){
                    Op op = IntegerAggregator.this.op;
                    r.setField(0,f);

                    if(op == Op.MIN){
                        r.setField(1,new IntField(s.min));
                    } else if(op == Op.MAX){
                        r.setField(1,new IntField(s.max));
                    } else if(op == Op.SUM){
                        r.setField(1,new IntField(s.sum));
                    } else if(op == Op.AVG){
                        r.setField(1,new IntField(s.avg()));
                    } else {
                        r.setField(1,new IntField(s.count));
                    }
                }

                public void open()
                    throws DbException, TransactionAbortedException{
                    if(opened) return;
                    opened = true;

                    iter = IntegerAggregator.this.gb_results.entrySet().iterator();
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
                    Stats i = (Stats)entry.getValue();

                    Tuple r = new Tuple(IntegerAggregator.this.td);
                    fillValue(gf,i,r);

                    return r;
                }

                public void rewind() throws DbException, TransactionAbortedException{
                    if(!opened) throw new DbException("not opened");
                    if(closed) throw new DbException("closed");

                    iter = IntegerAggregator.this.gb_results.entrySet().iterator();
                }

                public TupleDesc getTupleDesc(){
                    return IntegerAggregator.this.td;
                }

                public void close(){
                    closed = true;
                    iter = null;
                }
            };
            
        }
    }

}
