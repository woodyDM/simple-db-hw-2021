package simpledb.common;

import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;

public class SingleValueOpIterator extends AbstractOpIterator {
    private final Tuple t;
    private boolean end = false;

    public SingleValueOpIterator(Tuple t) {
        super(t.getTupleDesc());
        this.t = t;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        end = false;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (end) {
            return null;
        }
        end = true;
        return t;
    }
}
