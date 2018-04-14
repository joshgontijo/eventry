package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;
import io.joshworks.fstore.serializer.StandardSerializer;

import java.nio.ByteBuffer;

public class NodeSerializer<K extends Comparable<K>, V> implements Serializer<Node<K, V>> {

    private final Serializer<LeafNode<K, V>> leafNodeSerializer;
    private final Serializer<InternalNode<K, V>> internalNodeSerializer;
    private final int order;
    private BlockStore<K, V> store;

    public NodeSerializer(int blockSize, int order, BlockStore<K, V> store, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.order = order;
        this.store = store;
        this.leafNodeSerializer = new LeafNodeSerializer(blockSize, keySerializer, valueSerializer);
        this.internalNodeSerializer = new InternalNodeSerializer(blockSize, keySerializer);
    }

    @Override
    public ByteBuffer toBytes(Node<K, V> data) {
        switch (data.type) {
            case Node.INTERNAL_NODE:
                return internalNodeSerializer.toBytes((InternalNode<K, V>) data);
            case Node.LEAF_NODE:
                return leafNodeSerializer.toBytes((LeafNode<K, V>) data);
            default:
                throw new RuntimeException("Invalid node type " + data.type);
        }
    }

    @Override
    public Node<K, V> fromBytes(ByteBuffer buffer) {
        int type = buffer.getInt();
        switch (type) {
            case Node.INTERNAL_NODE:
                return internalNodeSerializer.fromBytes(buffer);
            case Node.LEAF_NODE:
                return leafNodeSerializer.fromBytes(buffer);
            default:
                throw new RuntimeException("Invalid node type " + type);
        }
    }

    public class InternalNodeSerializer implements Serializer<InternalNode<K, V>> {

        private final int blockSize;
        private final Serializer<K> keySerializer;
        private final Serializer<Integer> pointerSerializer = StandardSerializer.INTEGER;

        public InternalNodeSerializer(int blockSize, Serializer<K> keySerializer) {
            this.blockSize = blockSize;
            this.keySerializer = keySerializer;
        }


        @Override
        public ByteBuffer toBytes(InternalNode<K, V> data) {
            ByteBuffer bb = ByteBuffer.allocate(blockSize);

            bb.putInt(data.type);
            bb.putInt(data.id());
            bb.putInt(data.keys.size());

            for (int i = 0; i < data.keys.size(); i++) {
                bb.put(keySerializer.toBytes(data.keys.get(i)));
            }
            for (int i = 0; i < data.children.size(); i++) {
                bb.put(pointerSerializer.toBytes(data.children.get(i)));
            }

            return (ByteBuffer) bb.flip();
        }

        @Override
        public InternalNode<K, V> fromBytes(ByteBuffer buffer) {
            InternalNode<K, V> internalNode = new InternalNode<>(order, store);

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

    public class LeafNodeSerializer implements Serializer<LeafNode<K, V>> {

        private final int blockSize;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;

        public LeafNodeSerializer(int blockSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.blockSize = blockSize;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public ByteBuffer toBytes(LeafNode<K, V> data) {
            ByteBuffer bb = ByteBuffer.allocate(blockSize);
            bb.putInt(data.type);
            bb.putInt(data.id());
            bb.putInt(data.keys.size());

            for (int i = 0; i < data.keys.size(); i++) {
                bb.put(keySerializer.toBytes(data.keys.get(i)));
            }
            for (int i = 0; i < data.values.size(); i++) {
                bb.put(valueSerializer.toBytes(data.values.get(i)));
            }

            return (ByteBuffer) bb.flip();
        }

        @Override
        public LeafNode<K, V> fromBytes(ByteBuffer buffer) {
            LeafNode<K, V> leafNode = new LeafNode<>(order, store);

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