package simpledb.execution;

import simpledb.common.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private Integer preV;
    private int counter = 0;
    private final Map<String, Integer> values;
    private final Map<String, Integer> counters;

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.values = hasGroup() ? new LinkedHashMap<>() : null;
        this.counters = hasGroup() && what == Op.AVG ? new LinkedHashMap<>() : null;
    }

    private boolean hasGroup() {
        return gbfield != Aggregator.NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        counter++;

        Integer cur = Integer.parseInt(tup.getField(afield).toString());
        if (hasGroup()) {
            String key = tup.getField(gbfield).toString();
            Integer pre = values.get(key);
            values.put(key, operate(pre, cur));
            if (what == Op.AVG) {
                Integer cc = counters.getOrDefault(key, 0);
                counters.put(key, cc + 1);
            }
        } else {
            preV = operate(preV, cur);
        }
    }

    private Integer operate(Integer pre, Integer cur) {
        switch (what) {
            case MAX:
                return pre == null ? cur : Math.max(pre, cur);
            case MIN:
                return pre == null ? cur : Math.min(pre, cur);
            case SUM:
            case AVG:
                return pre == null ? cur : pre + cur;
            case COUNT:
                return pre == null ? 1 : pre + 1;
            default:
                throw new UnsupportedOperationException("not support " + what);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (counter == 0) {
            return new EmptyOpIterator(thisTupleDesc());
        }
        if (!hasGroup()) {
            Tuple tuple = new Tuple(thisTupleDesc());
            int v = what == Op.AVG ? preV / counter : preV;
            tuple.setField(0, new IntField(v));
            return new SingleValueOpIterator(tuple);
        }
        return new AbstractOpIterator(thisTupleDesc()) {

            Iterator<Map.Entry<String, Integer>> it;

            @Override
            protected Tuple fetchNext() throws DbException, TransactionAbortedException {
                if (it == null) it = values.entrySet().iterator();
                if (!it.hasNext()) {
                    return null;
                }
                Map.Entry<String, Integer> entry = it.next();
                Integer value = entry.getValue();
                if (what == Op.AVG) {
                    value /= counters.get(entry.getKey());
                }
                Tuple tuple = new Tuple(thisTupleDesc());
                tuple.setField(0, Util.createField(gbfieldtype, entry.getKey()));
                tuple.setField(1, new IntField(value));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = null;
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
