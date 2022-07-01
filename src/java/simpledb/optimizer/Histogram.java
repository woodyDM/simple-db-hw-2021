package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;

/**
 *
 */
 
public interface Histogram {
    
    double estimate(Predicate.Op op, Field field);

    void addValue(Field field);

}
