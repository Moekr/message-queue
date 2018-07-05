package com.moekr.mq;

import io.openmessaging.QueueStore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

import static com.moekr.mq.Constants.*;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

class QueueRegion extends QueueStore {
    private final FileChannel channel;
    private final Map<Integer, Buffer> bufferMap;
    private final Map<String, List<Block>> queueMap;

    QueueRegion(int index) throws IOException {
        channel = new RandomAccessFile(DATA_DIR + index, "rw").getChannel();
        bufferMap = new HashMap<>();
        queueMap = new HashMap<>();
    }

    private MappedByteBuffer fetchBuffer(Block block, MapMode mode) throws IOException {
        int bufferIndex = block.index / BLOCK_PER_BUFFER;
        Buffer buffer = bufferMap.get(bufferIndex);
        if (buffer != null && (mode == READ_ONLY || mode == buffer.mode)) {
            return buffer.touch().buffer;
        }
        while (bufferMap.size() >= MAX_LOADED_BUFFER) {
            int removeIndex = bufferMap.entrySet().stream()
                    .reduce((a, b) -> a.getValue().lastUsedAt > b.getValue().lastUsedAt ? b : a)
                    .orElseThrow(IllegalStateException::new)
                    .getKey();
            buffer = bufferMap.remove(removeIndex);
            if (buffer.mode == READ_WRITE) {
                buffer.buffer.force();
            }
        }
        MappedByteBuffer byteBuffer = channel.map(mode, bufferIndex * BUFFER_SIZE, BUFFER_SIZE);
        buffer = new Buffer(mode, byteBuffer);
        bufferMap.put(bufferIndex, buffer);
        return buffer.buffer;
    }

    @Override
    public synchronized void put(String queueName, byte[] message) {
        try {
            put0(queueName, message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void put0(String queueName, byte[] message) throws IOException {
        List<Block> blockList = queueMap.computeIfAbsent(queueName, s -> new ArrayList<>());
        if (blockList.isEmpty()) blockList.add(new Block());
        Block block = blockList.get(blockList.size() - 1);
        MappedByteBuffer buffer = fetchBuffer(block, READ_WRITE);
        buffer.position((block.index % BLOCK_PER_BUFFER) * BLOCK_SIZE + block.usedSlot * SLOT_SIZE);
        int left = SLOT_PER_BLOCK - block.usedSlot;
        if (left * SLOT_SIZE > message.length) {
            buffer.put(message);
            block.messageAmount++;
            block.usedSlot += message.length / SLOT_SIZE + 1;
            if (block.usedSlot == SLOT_PER_BLOCK) blockList.add(new Block());
        } else {
            buffer.put(message, 0, left * SLOT_SIZE);
            block.messageAmount++;
            block.usedSlot = SLOT_PER_BLOCK;
            block = new Block();
            blockList.add(block);
            buffer = fetchBuffer(block, READ_WRITE);
            buffer.position((block.index % BLOCK_PER_BUFFER) * BLOCK_SIZE);
            buffer.put(message, left * SLOT_SIZE, message.length - left * SLOT_SIZE);
            block.continuous = true;
            block.usedSlot = (message.length - left * SLOT_SIZE) / SLOT_SIZE + 1;
        }
    }

    @Override
    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {
        try {
            return get0(queueName, offset, num);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<byte[]> get0(String queueName, long offset, long num) throws IOException {
        List<Block> blockList = queueMap.get(queueName);
        if (blockList == null) return Collections.emptyList();
        MappedByteBuffer buffer;
        byte[] blockBuffer = new byte[BLOCK_SIZE];
        List<byte[]> result = new ArrayList<>();
        byte[] continuousHead = null;
        for (Block block : blockList) {
            if (num == 0) break;
            if (offset <= block.messageAmount) {
                buffer = fetchBuffer(block, READ_ONLY);
                buffer.position((block.index % BLOCK_PER_BUFFER) * BLOCK_SIZE);
                buffer.get(blockBuffer, 0, BLOCK_SIZE);
                long skip = (block.continuous ? 1 : 0) + offset;
                int previous = -1;
                for (int slotIndex = 0; slotIndex < SLOT_PER_BLOCK; slotIndex++) {
                    int slotEnd = (slotIndex + 1) * SLOT_SIZE - 1;
                    if (blockBuffer[slotEnd] == 0) {
                        if (skip > 0) {
                            skip--;
                        } else if (num > 0) {
                            int messageBegin = (previous + 1) * SLOT_SIZE;
                            if (blockBuffer[messageBegin] == 0) return result;
                            int messageEnd = slotEnd;
                            while (blockBuffer[messageEnd] == 0) {
                                messageEnd--;
                            }
                            byte[] message = Arrays.copyOfRange(blockBuffer, messageBegin, messageEnd + 1);
                            if (continuousHead != null) {
                                byte[] temp = new byte[continuousHead.length + message.length];
                                System.arraycopy(continuousHead, 0, temp, 0, continuousHead.length);
                                System.arraycopy(message, 0, temp, continuousHead.length, message.length);
                                result.add(temp);
                                continuousHead = null;
                            } else {
                                result.add(message);
                            }
                            num--;
                        } else {
                            break;
                        }
                        previous = slotIndex;
                    }
                }
                if (num > 0 && previous != SLOT_PER_BLOCK - 1) {
                    continuousHead = Arrays.copyOfRange(blockBuffer, (previous + 1) * SLOT_SIZE, BLOCK_SIZE);
                }
                offset = 0;
            } else {
                offset -= block.messageAmount;
            }
        }
        return result;
    }

    private int blockIndex = 0;

    private class Block {
        final int index;

        int messageAmount = 0;
        boolean continuous = false;
        int usedSlot = 0;

        Block() {
            this.index = blockIndex++;
        }
    }

    private class Buffer {
        final MapMode mode;
        final MappedByteBuffer buffer;

        long lastUsedAt;

        Buffer(MapMode mode, MappedByteBuffer buffer) {
            this.mode = mode;
            this.buffer = buffer;
            touch();
        }

        Buffer touch() {
            lastUsedAt = System.currentTimeMillis();
            return this;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        channel.close();
    }
}
