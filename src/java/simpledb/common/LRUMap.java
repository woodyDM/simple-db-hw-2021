package simpledb.common;

import java.util.*;

/**
 * 最近活跃的放tail
 *
 * @param <K>
 * @param <V>
 */
public class LRUMap<K, V> {
    private final Map<K, Node<K, V>> data = new HashMap<>();
    private final Node<K, V> dummyHead = new Node<>(null, null);
    private final int cap;
    private Node<K, V> tail;

    public LRUMap(int cap) {
        if (cap <= 1) {
            throw new IllegalArgumentException();
        }
        this.cap = cap;
    }

    public Set<K> keySet() {
        return data.keySet();
    }

    public List<Node<K, V>> toList() {
        List<Node<K, V>> list = new ArrayList<>();
        Node<K, V> cur = dummyHead.next;
        while (cur != null) {
            list.add(cur);
            cur = cur.next;
        }
        return list;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public V put(K key, V newValue) {
        V result = null;
        Node<K, V> old = data.get(key);
        if (old == null) {
            if (size() >= cap) {
                result = evict();
            }
            Node<K, V> node = new Node<>(key, newValue);
            data.put(key, node);
            addToTail(node);
        } else {
            old.v = newValue;
            moveToTail(old);
        }
        return result;
    }

    /**
     * add new node to tail
     *
     * @param node
     */
    private void addToTail(Node<K, V> node) {
        Node<K, V> pre = tail == null ? dummyHead : tail;
        tail = node;
        node.pre = pre;
        pre.next = node;
    }

    /**
     * mode node to tail
     *
     * @param node
     */
    private void moveToTail(Node<K, V> node) {
        cut(node);
        addToTail(node);
    }

    public V get(K key) {
        Node<K, V> node = data.get(key);
        if (node == null) {
            return null;
        }
        moveToTail(node);
        return node.v;
    }

    private void cut(Node<K, V> node) {
        node.pre.next = node.next;
        if (node.next != null) {
            node.next.pre = node.pre;
        }
        if (node.pre == dummyHead) {
            tail = null;
        } else if (node == tail) {
            tail = node.pre;
        }
        node.pre = null;
        node.next = null;

    }

    public V remove(K key) {
        Node<K, V> node = data.get(key);
        if (node == null) {
            return null;
        }
        cut(node);
        data.remove(key);
        return node.v;
    }

    public int size() {
        return data.size();
    }

    public V evict() {
        Node<K, V> result = dummyHead.next;
        cut(result);
        data.remove(result.k);
        return result.v;
    }

    public static class Node<K, V> {
        Node<K, V> next;
        Node<K, V> pre;
        final K k;
        V v;

        public Node(K k, V v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node<?, ?> node = (Node<?, ?>) o;

            if (k != null ? !k.equals(node.k) : node.k != null) return false;
            return v != null ? v.equals(node.v) : node.v == null;
        }

        @Override
        public int hashCode() {
            int result = k != null ? k.hashCode() : 0;
            result = 31 * result + (v != null ? v.hashCode() : 0);
            return result;
        }
    }

}
