package simpledb.common;

import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.storage.TupleDesc;

public abstract class AbstractOpIterator extends Operator {

    protected TupleDesc tupleDesc;

    public AbstractOpIterator(TupleDesc tupleDesc) {
        this.tupleDesc = tupleDesc;
    }

    @Override
    public OpIterator[] getChildren() {
        throw new UnsupportedOperationException("no need");
    }

    @Override
    public void setChildren(OpIterator[] children) {
        throw new UnsupportedOperationException("no need");
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }
}
