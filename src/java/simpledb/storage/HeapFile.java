package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int idx = pid.getPageNumber();
        if (idx < 0 || idx >= numPages()) {
            throw new IllegalArgumentException("invalid pid " + pid.getPageNumber());
        }
        try (FileInputStream ins = new FileInputStream(file);
             BufferedInputStream bi = new BufferedInputStream(ins)) {
            long skip = bi.skip((long) idx * BufferPool.getPageSize());
            byte[] buf = new byte[BufferPool.getPageSize()];
            bi.read(buf, 0, BufferPool.getPageSize());
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return Util.ceil(file.length(), BufferPool.getPageSize());

    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        int totalPage = numPages();
        BufferPool pool = Database.getBufferPool();

        return new AbstractDbFileIterator() {
            int pageIdx = -1;
            Iterator<Tuple> it;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (pageIdx == -1) {
                    return null;
                }
                if (pageIdx == -2) {
                    throw new NoSuchElementException();
                }
                if (it == null || !it.hasNext()) {
                    if (pageIdx < totalPage) {
                        HeapPage page = (HeapPage) pool.getPage(tid, new HeapPageId(getId(), pageIdx++), null);
                        it = page.iterator();
                    } else {
                        return null;
                    }
                }
                return it.next();
            }

            @Override
            public void close() {
                super.close();
                pageIdx = -2;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageIdx = 0;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageIdx = -1;
                it = null;
            }
        };
    }

}

