package simpledb.execution;

import simpledb.common.AbstractOpIterator;
import simpledb.common.DbException;
import simpledb.common.SingleValueOpIterator;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<String, Integer> map;
    private int c = 0;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.map = hasGroup() ? new LinkedHashMap<>() : null;
    }

    private boolean hasGroup() {
        return gbfield != Aggregator.NO_GROUPING;
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("string only support COUNT");
        }
        if (hasGroup()) {
            Field gf = tup.getField(gbfield);
            Integer pre = map.getOrDefault(gf.toString(), 0);
            map.put(gf.toString(), pre + 1);
        } else {
            c++;
        }
        // some code goes here
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if (!hasGroup()) {
            Tuple t = new Tuple(thisTupleDesc());
            t.setField(0, new IntField(c));
            return new SingleValueOpIterator(t);
        }
        //has group
        return new AbstractOpIterator(thisTupleDesc()) {

            private Iterator<Map.Entry<String, Integer>> mapIt;

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                mapIt = null;
            }

            @Override
            protected Tuple fetchNext() throws DbException, TransactionAbortedException {
                if (mapIt == null) {
                    mapIt = map.entrySet().iterator();
                }
                if (mapIt.hasNext()) {
                    Tuple t = new Tuple(thisTupleDesc());
                    Map.Entry<String, Integer> en = mapIt.next();
                    t.setField(0, Util.createField(gbfieldtype, en.getKey()));
                    t.setField(1, new IntField(en.getValue()));
                    return t;
                }
                return null;
            }
        };
    }

    private TupleDesc thisTupleDesc() {
        if (hasGroup()) {
            return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

}
