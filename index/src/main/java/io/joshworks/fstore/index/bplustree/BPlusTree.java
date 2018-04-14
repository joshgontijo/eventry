package io.joshworks.fstore.index.bplustree;


import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;
import io.joshworks.fstore.index.bplustree.storage.HeapBlockStore;
import io.joshworks.fstore.index.bplustree.storage.OffHeapBlockStore;
import io.joshworks.fstore.index.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.bplustree.util.InsertResult;
import io.joshworks.fstore.index.bplustree.util.Result;
import io.joshworks.fstore.serializer.StandardSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BPlusTree<K extends Comparable<K>, V> implements Tree<K, V> {

    /**
     * The branching factor used when none specified in constructor.
     */
    private static final int DEFAULT_BRANCHING_FACTOR = 128;

    /**
     * The branching factor for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */
    final int order;
    private int rootId;
    private final BlockStore store;
    private int size;
    private int height;


    private BPlusTree(BlockStore store, int order) {
        this.order = order;
        this.store = store;
        this.rootId = store.placeBlock(new LeafNode());
    }

    public BPlusTree(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.order = order;
        int blockSize = 4096; //TODO this is terrible
        this.store = new OffHeapBlockStore(blockSize, new NodeSerializer(blockSize, keySerializer, valueSerializer));
        this.rootId = store.placeBlock(new LeafNode());
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> heapTree(int order) {
        if (order <= 2) {
            throw new IllegalArgumentException("B+Tree order must be greater than 2");
        }
        return new BPlusTree<>(new HeapBlockStore(), order);
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> heapTree() {
        return heapTree(DEFAULT_BRANCHING_FACTOR);
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> offHeapTree(int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if (order <= 2) {
            throw new IllegalArgumentException("B+Tree order must be greater than 2");
        }

        return new BPlusTree<>(order, keySerializer, valueSerializer);
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> offHeapTree(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return offHeapTree(DEFAULT_BRANCHING_FACTOR, keySerializer, valueSerializer);
    }

    /**
     * Adds a new entry to the index, replacing if the key already exist
     *
     * @param key   The key to be inserted
     * @param value The value to be inserted
     * @return The previous value associated with this key
     * @throws IllegalArgumentException If the key is null
     */
    @Override
    public V put(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        Node root = store.readBlock(rootId);
        InsertResult<V> insertResult = root.insertValue(key, value, root);
        if (insertResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = insertResult.newRootId;
            height++;
        }
        if (insertResult.foundValue() == null)
            size++;
        return insertResult.foundValue();
    }

    @Override
    public V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        Node root = store.readBlock(rootId);
        return root.getValue(key);
    }

    @Override
    public void clear() {
        store.clear();
        rootId = store.placeBlock(new LeafNode());
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return TreeIterator.iterator(store, rootId);
    }

    public Iterator<Entry<K, V>> iterator(Range<K> range) {
        return TreeIterator.iterator(store, rootId, range);
    }


    @Override
    public int height() {
        return height;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public V remove(K key) {
        Node root = store.readBlock(rootId);
        DeleteResult<V> deleteResult = root.deleteValue(key, root);
        if (deleteResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = deleteResult.newRootId;
            height--;
        }
        if (deleteResult.foundValue() != null)
            size--;
        return deleteResult.foundValue();
    }


    @Override
    public String toString() {
        Node root = store.readBlock(rootId);

        Queue<List<Node>> queue = new LinkedList<>();
        queue.add(Arrays.asList(root));
        StringBuilder sb = new StringBuilder();
        while (!queue.isEmpty()) {
            Queue<List<Node>> nextQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                List<Node> nodes = queue.remove();
                sb.append('{');
                Iterator<Node> it = nodes.iterator();
                while (it.hasNext()) {
                    Node node = it.next();
                    sb.append(node.toString());
                    if (it.hasNext())
                        sb.append(", ");
                    if (node.type == Node.INTERNAL_NODE) {
                        List<Integer> children = ((InternalNode) node).children;

                        List<Node> childrenNodes = new ArrayList<>();
                        for (Integer childId : children) {
                            if (childId >= 0)
                                childrenNodes.add(store.readBlock(childId));
                        }
                        nextQueue.add(childrenNodes);
                    }

                }
                sb.append('}');
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append('\n');
            }
            queue = nextQueue;
        }

        return sb.toString();
    }

    public abstract class Node {

        protected static final int LEAF_NODE = 1;
        protected static final int INTERNAL_NODE = 2;

        private int id = -1;
        protected final int type;
        protected final List<K> keys;

        protected Node(int type) {
            this.keys = new ArrayList<>(order - 1);
            this.type = type;
        }


        abstract V getValue(K key);

        abstract DeleteResult<V> deleteValue(K key, Node root);

        abstract InsertResult<V> insertValue(K key, V value, Node root);

        abstract void merge(Node sibling);

        abstract Node split();

        abstract boolean isOverflow();

        abstract boolean isUnderflow();

        abstract Entry<K, V> getFirstEntry();

        public int id() {
            return id;
        }

        public void id(int id) {
            this.id = id;
        }

        public String toString() {
            return keys.toString();
        }

        protected int keyNumber() {
            return keys.size();
        }
    }

    public class InternalNode extends Node {

        final List<Integer> children;

        protected InternalNode() {
            super(Node.INTERNAL_NODE);
            this.children = new ArrayList<>(order);
        }


        @Override
        V getValue(K key) {
            return getChild(key).getValue(key);
        }

        @Override
        DeleteResult<V> deleteValue(K key, Node root) {
            Node child = getChild(key);

            DeleteResult<V> result = child.deleteValue(key, root);
            if (child.isUnderflow()) {
                Node childLeftSibling = getChildLeftSibling(key);
                Node left = childLeftSibling != null ? childLeftSibling : child;
                Node right = childLeftSibling != null ? child : getChildRightSibling(key);
                if (right == null)
                    throw new IllegalStateException("No right child available");

                left.merge(right);

                //DIRTY
                store.writeBlock(left.id, left);
                store.freeBlock(right.id);

                deleteChild(right.getFirstEntry().key);
                if (left.isOverflow()) {
                    Node sibling = left.split();

                    //DIRTY
                    store.writeBlock(left.id(), left);

                    insertChild(sibling.getFirstEntry().key, sibling);
                }
                if (root.keyNumber() == 0)
                    result.newRootId(left.id());
//                root = left;
            }
            return result;

        }

        @Override
        InsertResult<V> insertValue(K key, V value, Node root) {
            Node child = getChild(key);
            InsertResult<V> insertResult = child.insertValue(key, value, root);

            if (child.isOverflow()) {
                Node sibling = child.split();
                insertChild(sibling.getFirstEntry().key, sibling);
            }
            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstEntry().key);
                newRoot.children.add(this.id());
                newRoot.children.add(sibling.id());
//            root = newRoot;
                int id = store.placeBlock(newRoot);
                insertResult.newRootId(id);
            }
            return insertResult;
        }

        @Override
        void merge(Node sibling) {
            InternalNode node = (InternalNode) sibling;
            keys.add(node.getFirstEntry().key);
            keys.addAll(node.keys);
            children.addAll(node.children);
        }

        @Override
        Node split() {
            int from = keyNumber() / 2 + 1, to = keyNumber();
            InternalNode sibling = new InternalNode();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.children.addAll(children.subList(from, to + 1));

            keys.subList(from - 1, to).clear();
            children.subList(from, to + 1).clear();

            store.placeBlock(sibling);
            return sibling;
        }

        @Override
        boolean isOverflow() {
            return children.size() > order;
        }

        @Override
        boolean isUnderflow() {
            return children.size() < Math.ceil(order / 2);
        }

        @Override
        Entry<K, V> getFirstEntry() {
            return getChildByIndex(0).getFirstEntry();
        }

        private void deleteChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            if (loc >= 0) {
                keys.remove(loc);
                children.remove(loc + 1);

                //DIRTY
                store.writeBlock(id(), this);

            }
        }

        private void insertChild(K key, Node child) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                children.set(childIndex, child.id());
            } else {
                keys.add(childIndex, key);
                children.add(childIndex + 1, child.id());
            }
        }

        private Node getChildLeftSibling(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (childIndex > 0)
                return getChildByIndex(childIndex - 1);

            return null;
        }

        private Node getChildRightSibling(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (childIndex < keyNumber())
                return getChildByIndex(childIndex + 1);

            return null;
        }

        Node getChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            return getChildByIndex(childIndex);
        }

        private Node getChildByIndex(int idx) {
            Integer childId = children.get(idx);
            return store.readBlock(childId);
        }

    }

    public class LeafNode extends Node {

        private final List<V> values;
        private int next = -1;
        private int previous; //TODO


        protected LeafNode() {
            super(Node.LEAF_NODE);
            //It's actually order - 1, but to avoid resizing on overflow we leave with one more
            this.values = new ArrayList<>(order);
        }

        public int next() {
            return next;
        }

        @Override
        V getValue(K key) {
            int loc = Collections.binarySearch(keys, key);
            return loc >= 0 ? values.get(loc) : null;
        }

        @Override
        DeleteResult<V> deleteValue(K key, Node root) {
            int loc = Collections.binarySearch(keys, key);
            DeleteResult<V> deleteResult = new DeleteResult<>();
            if (loc >= 0) {
                keys.remove(loc);
                V removed = values.remove(loc);
                deleteResult.deleted(removed);

                //DIRTY
                store.writeBlock(this.id(), this);

            }
            return deleteResult;
        }

        @Override
        InsertResult<V> insertValue(K key, V value, Node root) {
            int loc = Collections.binarySearch(keys, key);
            int valueIndex = loc >= 0 ? loc : -loc - 1;

            InsertResult<V> insertResult = new InsertResult<>();

            //TODO check duplicated values
            if (loc >= 0) {
                V v = values.get(valueIndex);
                insertResult.previousValue(v);
                values.set(valueIndex, value);
            } else {
                keys.add(valueIndex, key);
                values.add(valueIndex, value);
            }

            //DIRTY
            store.writeBlock(id(), this);

            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstEntry().key);
                newRoot.children.add(this.id());
                newRoot.children.add(sibling.id());
                //root = newRoot;
                int id = store.placeBlock(newRoot);
                return insertResult.newRootId(id);
            }
            return insertResult;
        }


        @Override
        void merge(Node sibling) {
            LeafNode node = (LeafNode) sibling;
            keys.addAll(node.keys);
            values.addAll(node.values);
            next = node.next;
        }

        @Override
        Node split() {
            //TODO change BlockStore to accept an already populated block and no id, the method stores the new block, set the id and return the id
            LeafNode sibling = new LeafNode();
            int from = (keyNumber() + 1) / 2, to = keyNumber();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.values.addAll(values.subList(from, to));

            keys.subList(from, to).clear();
            values.subList(from, to).clear();

            sibling.next = next;
            next = store.placeBlock(sibling);
            return sibling;
        }

        @Override
        boolean isOverflow() {
            return values.size() > order - 1;
        }

        @Override
        boolean isUnderflow() {
            return values.size() > order - 1;
        }

        @Override
        Entry<K, V> getFirstEntry() {
            return Entry.of(keys.get(0), values.get(0));
        }

        public List<Entry<K, V>> entries() {
            List<Entry<K, V>> entries = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                entries.add(Entry.of(keys.get(i), values.get(i)));
            }
            return entries;
        }
    }

    public class NodeSerializer implements Serializer<BPlusTree.Node> {

        private final Serializer<BPlusTree.LeafNode> leafNodeSerializer;
        private final Serializer<BPlusTree.InternalNode> internalNodeSerializer;

        public NodeSerializer(int blockSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.leafNodeSerializer = new LeafNodeSerializer(blockSize, keySerializer, valueSerializer);
            this.internalNodeSerializer = new InternalNodeSerializer(blockSize, keySerializer);
        }

        @Override
        public ByteBuffer toBytes(BPlusTree.Node data) {
            switch (data.type) {
                case BPlusTree.Node.INTERNAL_NODE:
                    return internalNodeSerializer.toBytes((BPlusTree.InternalNode) data);
                case BPlusTree.Node.LEAF_NODE:
                    return leafNodeSerializer.toBytes((BPlusTree.LeafNode) data);
                default:
                    throw new RuntimeException("Invalid node type " + data.type);
            }
        }

        @Override
        public BPlusTree.Node fromBytes(ByteBuffer buffer) {
            int type = buffer.getInt();
            switch (type) {
                case BPlusTree.Node.INTERNAL_NODE:
                    return internalNodeSerializer.fromBytes(buffer);
                case BPlusTree.Node.LEAF_NODE:
                    return leafNodeSerializer.fromBytes(buffer);
                default:
                    throw new RuntimeException("Invalid node type " + type);
            }
        }

        public class InternalNodeSerializer implements Serializer<BPlusTree.InternalNode> {

            private final int blockSize;
            private final Serializer<K> keySerializer;
            private final Serializer<Integer> pointerSerializer = StandardSerializer.INTEGER;

            public InternalNodeSerializer(int blockSize, Serializer<K> keySerializer) {
                this.blockSize = blockSize;
                this.keySerializer = keySerializer;
            }


            @Override
            public ByteBuffer toBytes(BPlusTree.InternalNode data) {
                ByteBuffer bb = ByteBuffer.allocate(blockSize);

                bb.putInt(data.id());
                bb.putInt(data.type);
                bb.putInt(data.keys.size());

                for (int i = 0; i < data.keys.size(); i++) {
                    bb.put(keySerializer.toBytes((K) data.keys.get(i)));
                }
                for (int i = 0; i < data.children.size(); i++) {
                    bb.put(pointerSerializer.toBytes((int) data.children.get(i)));
                }

                return (ByteBuffer) bb.flip();
            }

            @Override
            public BPlusTree.InternalNode fromBytes(ByteBuffer buffer) {
                InternalNode internalNode = new InternalNode();

                int id = buffer.getInt();
                internalNode.id(id);

                int keys = buffer.getInt();

                for (int i = 0; i < keys; i++) {
                    internalNode.keys.add(keySerializer.fromBytes(buffer));
                }
                if (keys > 0) {
                    for (int i = 0; i < keys + 1; i++) {
                        internalNode.children.add(pointerSerializer.fromBytes(buffer));
                    }
                }

                return internalNode;
            }
        }

        public class LeafNodeSerializer implements Serializer<BPlusTree.LeafNode> {

            private final int blockSize;
            private final Serializer<K> keySerializer;
            private final Serializer<V> valueSerializer;

            public LeafNodeSerializer(int blockSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
                this.blockSize = blockSize;
                this.keySerializer = keySerializer;
                this.valueSerializer = valueSerializer;
            }

            @Override
            public ByteBuffer toBytes(BPlusTree.LeafNode data) {
                ByteBuffer bb = ByteBuffer.allocate(blockSize);
                bb.putInt(data.type);
                bb.putInt(data.id());
                bb.putInt(data.keys.size());

                for (int i = 0; i < data.keys.size(); i++) {
                    bb.put(keySerializer.toBytes((K) data.keys.get(i)));
                }
                for (int i = 0; i < data.values.size(); i++) {
                    bb.put(valueSerializer.toBytes((V) data.values.get(i)));
                }

                return (ByteBuffer) bb.flip();
            }

            @Override
            public BPlusTree.LeafNode fromBytes(ByteBuffer buffer) {
                LeafNode leafNode = new LeafNode();

                int id = buffer.getInt();
                leafNode.id(id);

                int keys = buffer.getInt();

                for (int i = 0; i < keys; i++) {
                    leafNode.keys.add(keySerializer.fromBytes(buffer));
                }
                for (int i = 0; i < keys; i++) {
                    leafNode.values.add(valueSerializer.fromBytes(buffer));
                }

                return leafNode;
            }
        }
    }

}
