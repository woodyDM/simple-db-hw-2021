package simpledb.common;

import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public class EmptyOpIterator extends AbstractOpIterator{
    public EmptyOpIterator(TupleDesc tupleDesc) {
        super(tupleDesc);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return null;
    }
}
