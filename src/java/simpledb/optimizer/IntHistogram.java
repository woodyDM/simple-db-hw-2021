package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

    final int[] counter;
    private final int min;
    private final int max;
    private final int _max;
    private int size;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.counter = new int[buckets];
        this.min = min;
        this.max = max;
        this._max = max + 1;
        this.size = 0;
    }


    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int i = indexOf(v);
        if (i < 0) {
            throw new IllegalArgumentException("");
        }
        this.counter[i]++;
        size++;
    }

    @Override
    public void addValue(Field field) {
        this.addValue(Integer.parseInt(field.toString()));
    }

    private int indexOf(int v) {
        return (int)((long)counter.length * (v - min) / (_max - min));
    }

    @Override
    public double estimate(Predicate.Op op, Field field) {
        return this.estimateSelectivity(op, Integer.parseInt(field.toString()));
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int idx = indexOf(v);

        switch (op) {
            case EQUALS:
                return 1.0 * getCounterV(idx) / size;
            case NOT_EQUALS:
                return 1.0 - 1.0 * getCounterV(idx) / size;
            case LESS_THAN:
                return 1.0 * totalUpToOf2(idx, true) / size;
            case LESS_THAN_OR_EQ:
                return 1.0 * totalUpToOf2(idx, false) / size;
            case GREATER_THAN:
                return 1.0 - 1.0 * totalUpToOf2(idx, false) / size;
            case GREATER_THAN_OR_EQ:
                return 1.0 - 1.0 * totalUpToOf2(idx, true) / size;
            default:
                throw new UnsupportedOperationException("to do");
        }
    }

    private int getCounterV(int idx) {
        if (idx < 0 || idx >= counter.length) return 0;
        return counter[idx];
    }

    private int totalUpToOf2(int idx, boolean exclude) {
        if (idx < 0) return 0;
        if (idx >= counter.length) return size;
        return totalUpToOf(idx, exclude);
    }

    private int totalUpToOf(int idx, boolean exclude) {
        int b = exclude ? idx : idx + 1;
        int c = 0;
        for (int i = 0; i < b && i < counter.length; i++) {
            c += counter[i];
        }
        return c;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("IntHistogram{");
        sb.append("counter=");
        sb.append('[');
        for (int i = 0; i < counter.length; ++i)
            sb.append(i == 0 ? "" : ", ").append(counter[i]);
        sb.append(']');
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", _max=").append(_max);
        sb.append('}');
        return sb.toString();
    }
}
