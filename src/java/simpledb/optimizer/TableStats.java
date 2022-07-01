package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 *
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Compute table stats Done.");
    }


    private Histogram[] histograms;
    private int size = 0;
    private final int ioCostPerPage;
    private final DbFile dbFile;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */

    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        init();
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    }

    private void init() {
        DbFile file = this.dbFile;
        int colums = file.getTupleDesc().numFields();
        DbFileIterator it = file.iterator(new TransactionId());

        Stat[] stats = new Stat[colums];
        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                size++;
                for (int i = 0; i < colums; i++) {
                    if (stats[i] == null) {
                        stats[i] = new Stat();
                    }
                    Field field = t.getField(i);
                    stats[i].append(field);
                }
            }
            //create histograms
            this.histograms = new Histogram[colums];
            for (int i = 0; i < colums; i++) {
                this.histograms[i] = stats[i].toHistogram();
            }
            //scan again to estimate
            it.rewind();
            while (it.hasNext()) {
                Tuple t = it.next();
                for (int i = 0; i < colums; i++) {
                    this.histograms[i].addValue(t.getField(i));
                }
            }
            it.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        int numTuplesPerPage = Util.getNumTuplesPerPage(dbFile.getTupleDesc().getSize());
        int pages = Util.ceil(size, numTuplesPerPage);
        return pages * 2 * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * size);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        //todo ?
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        return histograms[field].estimate(op, constant);
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return size;
    }

    static class Stat {
        Field min;
        Field max;

        Histogram toHistogram() {
            if (min != null) {
                if (min.getType() == Type.INT_TYPE) {
                    return new IntHistogram(NUM_HIST_BINS, Integer.parseInt(min.toString()), Integer.parseInt(max.toString()));
                } else {
                    return new StringHistogram(NUM_HIST_BINS);
                }
            }
            System.err.println("WARN: histopgram empty");
            return new StringHistogram(NUM_HIST_BINS);
        }

        void append(Field t) {
            if (min == null) {
                min = t;
                max = t;
                return;
            }
            if (t.compare(Predicate.Op.LESS_THAN, min)) min = t;
            if (t.compare(Predicate.Op.GREATER_THAN, max)) max = t;
        }
    }


}
