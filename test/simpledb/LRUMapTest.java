package simpledb;

import org.junit.Test;
import simpledb.common.LRUMap;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class LRUMapTest {

    @Test
    public void shouldCap() {
        LRUMap<Integer, String> map = new LRUMap<>(3);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        map.put(4, "4");

        String s = map.get(1);

        assertNull(s);
    }

    @Test
    public void shouldCap2() {
        LRUMap<Integer, String> map = new LRUMap<>(3);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        map.put(1, "11");
        String evict = map.put(4, "4");

        String s = map.get(1);
        
        assertEquals(evict, "2");
        assertEquals(s, "11");
    }

    @Test
    public void shouldCap3() {
        LRUMap<Integer, String> map = new LRUMap<>(3);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
    
        String evict = map.remove(2);
        List<LRUMap.Node<Integer, String>> list = map.toList();
        
        assertEquals(map.size(),2);
        assertEquals(evict, "2");
        assertEquals(list.size(),2);
        assertEquals(list.get(0),new LRUMap.Node<>(1,"1"));
        assertEquals(list.get(1),new LRUMap.Node<>(3,"3"));
        
    }

    @Test
    public void shouldCap4() {
        LRUMap<Integer, String> map = new LRUMap<>(3);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        String evict = map.remove(2);
        map.get(1);
        List<LRUMap.Node<Integer, String>> list = map.toList();

        assertEquals(map.size(),2);
        assertEquals(evict, "2");
        assertEquals(list.size(),2);
        assertEquals(list.get(0),new LRUMap.Node<>(3,"3"));
        assertEquals(list.get(1),new LRUMap.Node<>(1,"1"));

    }

    @Test
    public void shouldCap5() {
        LRUMap<Integer, String> map = new LRUMap<>(3);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        String put = map.put(4, "4");
        map.put(5, "5");
        String evict = map.remove(4);
        map.put(6, "6");
        map.put(7, "7");

        map.get(5);
        List<LRUMap.Node<Integer, String>> list = map.toList();

        assertEquals(put,"1");
        assertEquals(map.size(),3);
        assertEquals(list.size(),3);
        assertEquals(list.get(0),new LRUMap.Node<>(6,"6"));
        assertEquals(list.get(1),new LRUMap.Node<>(7,"7"));
        assertEquals(list.get(2),new LRUMap.Node<>(5,"5"));
    }
}
