package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private int tableid;
    private String tableAlias;
    private TupleDesc tupleDesc;
    private String realTableName;
    private DbFileIterator it;
    private DbFile dbFile;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        _load(tableid, tableAlias);
    }


    private void _load(int tableid, String tableAlias) {
        this.tableid = tableid;
        TupleDesc real = Database.getCatalog().getTupleDesc(tableid);
        this.tupleDesc = real.addTableAlias(tableAlias);
        this.realTableName = Database.getCatalog().getTableName(tableid);
        this.tableAlias = tableAlias;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return realTableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        _load(tableid,tableAlias);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        it = dbFile.iterator(tid);
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return it!=null&&it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) {
            throw new NoSuchElementException("it null");
        }
        return it.next();
    }

    public void close() {
        if (it != null) {
            it.close();
        }
        // some code goes here
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (it != null) {
            it.rewind();
        }
    }
}
