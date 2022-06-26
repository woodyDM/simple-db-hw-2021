package simpledb.storage;

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
}
