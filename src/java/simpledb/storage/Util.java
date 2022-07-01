package simpledb.storage;

import simpledb.common.Type;

public class Util {

    public static int ceil(int total, int div) {
        int v = total / div;
        if (v * div < total) v++;
        return v;
    }

    public static int ceil(long total, int div) {
        long v = total / div;
        if (v * div < total) v++;
        return (int) v;
    }

    public static Field createField(Type t, String v) {
        return t == Type.INT_TYPE ? new IntField(Integer.parseInt(v)) : new StringField(v);
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    public static int getNumTuplesPerPage(int tupleDescSize) {
        // some code goes here
        return (BufferPool.getPageSize() * 8) / (tupleDescSize * 8 + 1);
    }
    
}
