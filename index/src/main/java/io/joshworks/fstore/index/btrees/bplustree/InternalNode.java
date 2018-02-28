package io.joshworks.fstore.index.btrees.bplustree;


import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.btrees.bplustree.util.InsertResult;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalNode<K extends Comparable<K>, V> extends Node<K, V> {

    final List<Integer> children;

    protected InternalNode(BlockStore<Node<K, V>> store, int order) {
        super(store, order);
        this.children = new ArrayList<>(order);
    }

    @Override
    V getValue(K key) {
        return getChild(key).getValue(key);
    }

    @Override
    DeleteResult<V> deleteValue(K key, Node<K, V> root) {
        Node<K, V> child = getChild(key);

        DeleteResult<V> result = child.deleteValue(key, root);
        if (child.isUnderflow()) {
            Node<K, V> childLeftSibling = getChildLeftSibling(key);
            Node<K, V> left = childLeftSibling != null ? childLeftSibling : child;
            Node<K, V> right = childLeftSibling != null ? child : getChildRightSibling(key);
            left.merge(right);
            deleteChild(right.getFirstEntry().key);
            if (left.isOverflow()) {
                Node<K, V> sibling = left.split();
                insertChild(sibling.getFirstEntry().key, sibling);
            }
            if (root.keyNumber() == 0)
                result = DeleteResult.of(left.id(), result.deleted);
//                root = left;
        }
        return result;

    }

    @Override
    InsertResult insertValue(K key, V value, Node<K, V> root) {
        Node<K, V> child = getChild(key);
        InsertResult insertResult = child.insertValue(key, value, root);

        if (child.isOverflow()) {
            Node<K, V> sibling = child.split();
            insertChild(sibling.getFirstEntry().key, sibling);
        }
        if (root.isOverflow()) {
            Node<K, V> sibling = split();
            InternalNode<K, V> newRoot = Node.allocateInternal(store, order);
            newRoot.keys.add(sibling.getFirstEntry().key);
            newRoot.children.add(this.id());
            newRoot.children.add(sibling.id());
//            root = newRoot;
            int id = store.placeBlock(newRoot);
            insertResult = InsertResult.of(id, insertResult.inserted);
        }
        return insertResult;
    }

    @Override
    void merge(Node<K, V> sibling) {
        InternalNode<K, V> node = (InternalNode<K, V>) sibling;
        keys.add(node.getFirstEntry().key);
        keys.addAll(node.keys);
        children.addAll(node.children);
    }

    @Override
    Node<K, V> split() {
        int from = keyNumber() / 2 + 1, to = keyNumber();
        InternalNode<K, V> sibling = Node.allocateInternal(store, order);
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
        }
    }

    private void insertChild(K key, Node<K, V> child) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (loc >= 0) {
            children.set(childIndex, child.id());
        } else {
            keys.add(childIndex, key);
            children.add(childIndex + 1, child.id());
        }
    }

    private Node<K, V> getChildLeftSibling(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (childIndex > 0)
            return getChildByIndex(childIndex - 1);

        return null;
    }

    private Node<K, V> getChildRightSibling(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (childIndex < keyNumber())
            return getChildByIndex(childIndex + 1);

        return null;
    }

    Node<K, V> getChild(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        return getChildByIndex(childIndex);
    }

    private Node<K, V> getChildByIndex(int idx) {
        Integer childId = children.get(idx);
        return store.readBlock(childId);
    }


}
