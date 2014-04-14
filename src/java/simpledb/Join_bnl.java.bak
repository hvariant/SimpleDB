package simpledb;

import java.util.*;
import java.io.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    //@ADDED
    public JoinPredicate p = null;
    public DbIterator child1 = null;
    public DbIterator child2 = null;
    public DbIterator[] children = null;

    final public int BLOCK_SIZE = 100;

    public Map<String,LinkedList<String> > map_pool1 = null;
    public LinkedList<String> pool1 = null;
    public LinkedList<Tuple> results = null;
    //@ADDED

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) { //@ADDED
        System.out.println("BLOCK NESTED LOOP VERSION, BLOCK_SIZE=" + new Integer(BLOCK_SIZE) );

        this.p = p;
        this.child1 = child1;
        this.child2 = child2;

        this.children = new DbIterator[2];
        this.children[0] = child1;
        this.children[1] = child2;

        this.map_pool1 = new HashMap<String,LinkedList<String> >();
        this.pool1 = new LinkedList<String>();
        this.results = new LinkedList<Tuple>();
    }

    public JoinPredicate getJoinPredicate() { //@ADDED
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() { //@ADDED @TODO: quantified by alias of table name?
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() { //@ADDED @TODO: quantified by alias of table name?
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() { //@ADDED
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException { //@ADDED
        super.open();

        child1.open();
        child2.open();
    }

    public void close() { //@ADDED
        super.close();

        map_pool1.clear();
        pool1.clear();
        results.clear();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException { //@ADDED
        map_pool1.clear();
        pool1.clear();
        results.clear();
        child1.rewind();
        child2.rewind();
    }

    private String serializeToString(Serializable o) throws DbException, TransactionAbortedException { //@ADDED
        String ret = "";

        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream so = new ObjectOutputStream(bo);
            so.writeObject(o);
            so.flush();
            ret = bo.toString();
        } catch (Exception e) {
            throw new DbException("serializeToString:" + e);
        }

        return ret;
    }

    private Serializable deserializeFromString(String s) throws DbException, TransactionAbortedException { //@ADDED
        Serializable o = null;

        try {
            byte b[] = s.getBytes(); 
            ByteArrayInputStream bi = new ByteArrayInputStream(b);
            ObjectInputStream si = new ObjectInputStream(bi);
            o = (Serializable) si.readObject();
        } catch (Exception e) {
            throw new DbException("serializeToString:" + e);
        }

        return o;
    }

    private boolean blockFetch1() throws DbException, TransactionAbortedException { //@ADDED
        pool1.clear();
        map_pool1.clear();

        for(int i=0;i<BLOCK_SIZE;i++){
            if(!child1.hasNext()) break;
            Tuple tp = child1.next();
            Field f = tp.getField(p.getField1());

            pool1.addLast(serializeToString(tp));

            String key = serializeToString(f);
            String value = serializeToString(tp);

            LinkedList<String> tps = map_pool1.get(key);
            if(tps == null){
                tps = new LinkedList<String>();
                map_pool1.put(key,tps);
            }
            tps.addLast(value);
        }

        return pool1.size() > 0;
    }

    private Tuple mergeTuple(Tuple t1,Tuple t2){ //@ADDED
        TupleDesc jointDesc = getTupleDesc();
        Tuple jointTuple = new Tuple(jointDesc);

        int l1 = t1.getTupleDesc().numFields();
        int l2 = t2.getTupleDesc().numFields();
        for(int i=0;i<l1;i++){
            jointTuple.setField(i,t1.getField(i));
        }
        for(int i=0;i<l2;i++){
            jointTuple.setField(i+l1,t2.getField(i));
        }

        return jointTuple;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    protected Tuple fetchNext() throws TransactionAbortedException, DbException { //@ADDED
        while(results.size() == 0){
            if(pool1.size()==0){
                if(!blockFetch1()) return null;
            }
            assert pool1.size() > 0;
            
            Tuple t1,t2;
            t1 = t2 = null;

            if(!child2.hasNext()){
                if(!blockFetch1()) return null;

                child2.rewind();
            }
            assert child2.hasNext();
            assert pool1.size() > 0;

            t2 = child2.next();

            if(p.getOperator() != Predicate.Op.EQUALS){
                Iterator<String> iter = pool1.iterator();

                while(iter.hasNext()){
                    t1 = (Tuple)deserializeFromString(iter.next());

                    if(p.filter(t1,t2)){
                        results.addLast(mergeTuple(t1,t2));
                    }
                }
            } else {
                String key = serializeToString(t2.getField(p.getField2()));
                LinkedList<String> t1s = map_pool1.get(key);
                if(t1s == null){
                    continue;
                }

                Iterator<String> iter = t1s.iterator();
                while(iter.hasNext()){
                    String value = iter.next();

                    t1 = (Tuple)deserializeFromString(value);
                    if(p.filter(t1,t2)){
                        results.addLast(mergeTuple(t1,t2));
                    }
                }
            }
            
        }

        return results.removeFirst();
    }

    @Override
    public DbIterator[] getChildren() { //@ADDED
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) { //@ADDED
        assert children.length == 2;

        child1 = children[0];
        child2 = children[1];

        this.children[0] = child1;
        this.children[1] = child2;
    }
}
