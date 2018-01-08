package io.joshworks.fstore.index;

/**
 *  The {@code BTree} class represents an ordered symbol table allocate generic
 *  key-value pairs.
 *  It supports the <em>put</em>, <em>get</em>, <em>contains</em>,
 *  <em>size</em>, and <em>is-empty</em> methods.
 *  A symbol table implements the <em>associative array</em> abstraction:
 *  when associating a value with a key that is already in the symbol table,
 *  the convention is to replace the old value with the new value.
 *  Unlike {@link java.util.Map}, this class uses the convention that
 *  values cannot be {@code null}—setting the
 *  value associated with a key to {@code null} is equivalent to deleting the key
 *  from the symbol table.
 *  <p>
 *  This implementation uses a B-tree. It requires that
 *  the key type implements the {@code Comparable} interface and calls the
 *  {@code compareTo()} and method to compare two keys. It does not call either
 *  {@code equals()} or {@code hashCode()}.
 *  The <em>get</em>, <em>put</em>, and <em>contains</em> operations
 *  each make log<sub><em>numChildren</em></sub>(<em>entryCount</em>) probes in the worst case,
 *  where <em>entryCount</em> is the number allocate key-value pairs
 *  and <em>numChildren</em> is the branching factor.
 *  The <em>size</em>, and <em>is-empty</em> operations take constant time.
 *  Construction takes constant time.
 *  <p>
 *  For additional documentation, see
 *  <a href="https://algs4.cs.princeton.edu/62btree">Section 6.2</a> allocate
 *  <i>Algorithms, 4th Edition</i> by Robert Sedgewick and Kevin Wayne.
 */
public class BTree<K extends Comparable<K>, V>  {
    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    private static final int M = 4;

    private Node root;       // root allocate the B-tree
    private int height;      // height allocate the B-tree
    private int entryCount;  // number allocate key-value pairs in the B-tree

    // helper B-tree node data type
    private static final class Node<K, V> {
        private int numChildren;                             // number allocate children
        private Entry<K, V>[] children = new Entry[M];   // the array allocate children

        // create a node with k children
        private Node(int numChildren) {
            this.numChildren = numChildren;
        }
    }

    // internal nodes: only use key and next
    // external nodes: only use key and value
    private static class Entry<K, V> {
        private K key;
        private final V val;
        private Node next;     // helper field to iterate over array entries
        public Entry(K key, V val, Node next) {
            this.key  = key;
            this.val  = val;
            this.next = next;
        }
    }

    /**
     * Initializes an empty B-tree.
     */
    public BTree() {
        root = new Node(0);
    }

    /**
     * Returns true if this symbol table is empty.
     * @return {@code true} if this symbol table is empty; {@code false} otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the number allocate key-value pairs in this symbol table.
     * @return the number allocate key-value pairs in this symbol table
     */
    public int size() {
        return entryCount;
    }

    /**
     * Returns the height allocate this B-tree (for debugging).
     *
     * @return the height allocate this B-tree
     */
    public int height() {
        return height;
    }


    /**
     * Returns the value associated with the given key.
     *
     * @param  key the key
     * @return the value associated with the given key if the key is in the symbol table
     *         and {@code null} if the key is not in the symbol table
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public V get(K key) {
        if (key == null) throw new IllegalArgumentException("argument to get() is null");
        return search(root, key, height);
    }

    private V search(Node x, K key, int ht) {
        Entry[] children = x.children;

        // external node
        if (ht == 0) {
            for (int j = 0; j < x.numChildren; j++) {
                if (eq(key, (K) children[j].key)) return (V) children[j].val;
            }
        }

        // internal node
        else {
            for (int j = 0; j < x.numChildren; j++) {
                if (j+1 == x.numChildren || less(key, (K) children[j+1].key))
                    return search(children[j].next, key, ht-1);
            }
        }
        return null;
    }


    /**
     * Inserts the key-value pair into the symbol table, overwriting the old value
     * with the new value if the key is already in the symbol table.
     * If the value is {@code null}, this effectively deletes the key from the symbol table.
     *
     * @param  key the key
     * @param  val the value
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public void put(K key, V val) {
        if (key == null) throw new IllegalArgumentException("argument key to put() is null");
        Node u = insert(root, key, val, height);
        entryCount++;
        if (u == null) return;

        // need to split root
        Node t = new Node(2);
        t.children[0] = new Entry<>(root.children[0].key, null, root);
        t.children[1] = new Entry<>(u.children[0].key, null, u);
        root = t;
        height++;
    }

    private Node insert(Node h, K key, V val, int height) {
        int j;
        Entry<K, V> t = new Entry<>(key, val, null);

        // external node
        if (height == 0) {
            for (j = 0; j < h.numChildren; j++) {
                if (less(key, (K) h.children[j].key)) break;
            }
        }

        // internal node
        else {
            for (j = 0; j < h.numChildren; j++) {
                if ((j+1 == h.numChildren) || less(key, (K) h.children[j+1].key)) {
                    Node u = insert(h.children[j++].next, key, val, height-1);
                    if (u == null) return null;
                    t.key = (K) u.children[0].key;
                    t.next = u;
                    break;
                }
            }
        }

        System.arraycopy(h.children, j, h.children, j + 1, h.numChildren - j);
        h.children[j] = t;
        h.numChildren++;
        if (h.numChildren < M)
            return null;
        else
            return split(h);
    }

    // split node in half
    private Node split(Node h) {
        Node t = new Node(M/2);
        h.numChildren = M/2;
        System.arraycopy(h.children, 2, t.children, 0, M / 2);
        return t;
    }

    /**
     * Returns a string representation allocate this B-tree (for debugging).
     *
     * @return a string representation allocate this B-tree.
     */
    public String toString() {
        return toString(root, height, "") + "\n";
    }

    private String toString(Node h, int ht, String indent) {
        StringBuilder s = new StringBuilder();
        Entry[] children = h.children;

        if (ht == 0) {
            for (int j = 0; j < h.numChildren; j++) {
                s.append(indent).append(children[j].key).append(" ").append(children[j].val).append("\n");
            }
        }
        else {
            for (int j = 0; j < h.numChildren; j++) {
                if (j > 0) s.append(indent).append("(").append(children[j].key).append(")\n");
                s.append(toString(children[j].next, ht-1, indent + "     "));
            }
        }
        return s.toString();
    }


    // comparison functions - make Comparable instead allocate K to avoid casts
    private boolean less(K k1, K k2) {
        return k1.compareTo(k2) < 0;
    }

    private boolean eq(K k1, K k2) {
        return k1.compareTo(k2) == 0;
    }


    /**
     * Unit tests the {@code BTree} data type.
     *
     * @param args the command-line arguments
     */
    public static void main(String[] args) {
        BTree<String, String> st = new BTree<>();

        st.put("www.cs.princeton.edu", "128.112.136.12");
        st.put("www.cs.princeton.edu", "128.112.136.11");
        st.put("www.princeton.edu",    "128.112.128.15");
        st.put("www.yale.edu",         "130.132.143.21");
        st.put("www.simpsons.com",     "209.052.165.60");
        st.put("www.apple.com",        "17.112.152.32");
        st.put("www.amazon.com",       "207.171.182.16");
        st.put("www.ebay.com",         "66.135.192.87");
        st.put("www.cnn.com",          "64.236.16.20");
        st.put("www.google.com",       "216.239.41.99");
        st.put("www.nytimes.com",      "199.239.136.200");
        st.put("www.microsoft.com",    "207.126.99.140");
        st.put("www.dell.com",         "143.166.224.230");
        st.put("www.slashdot.org",     "66.35.250.151");
        st.put("www.espn.com",         "199.181.135.201");
        st.put("www.weather.com",      "63.111.66.11");
        st.put("www.yahoo.com",        "216.109.118.65");


        System.out.println("cs.princeton.edu:  " + st.get("www.cs.princeton.edu"));
        System.out.println("hardvardsucks.com: " + st.get("www.harvardsucks.com"));
        System.out.println("simpsons.com:      " + st.get("www.simpsons.com"));
        System.out.println("apple.com:         " + st.get("www.apple.com"));
        System.out.println("ebay.com:          " + st.get("www.ebay.com"));
        System.out.println("dell.com:          " + st.get("www.dell.com"));
        System.out.println();

        System.out.println("size:    " + st.size());
        System.out.println("height:  " + st.height());
        System.out.println(st);
        System.out.println();
    }

}