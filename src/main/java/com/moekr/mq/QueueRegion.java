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
    private final int index;
    private final FileChannel channel;
    private final Map<Integer, Buffer> bufferMap;
    private final byte[] blockBuffer = new byte[BLOCK_SIZE];
    private final Map<String, Queue> queueMap;

    QueueRegion(int index) throws IOException {
        this.index = index;
        channel = new RandomAccessFile(DATA_DIR + index, "rw").getChannel();
        bufferMap = new LinkedHashMap<>(MAX_LOADED_BUFFER, 1.0F, true);
        queueMap = new HashMap<>(INITIAL_QUEUE_MAP_SIZE, 1.0F);
    }

    private MappedByteBuffer fetchBuffer(Block block, MapMode mode) throws IOException {
        int bufferIndex = block.index / BLOCK_PER_BUFFER;
        Buffer buffer = bufferMap.get(bufferIndex);
        if (buffer != null && (mode == READ_ONLY || mode == buffer.mode)) {
            return buffer.buffer;
        }
        Set<Map.Entry<Integer, Buffer>> entries = bufferMap.entrySet();
        Iterator<Map.Entry<Integer, Buffer>> iterator = entries.iterator();
        while (bufferMap.size() >= MAX_LOADED_BUFFER && iterator.hasNext()) {
            Buffer removeBuffer = iterator.next().getValue();
            iterator.remove();
            ToolKit.unmap(removeBuffer.buffer);
            // System.out.println("Region[" + index + "] unmap buffer[" + removeIndex + "] with mode " + removeBuffer.mode);
        }
        MappedByteBuffer byteBuffer = channel.map(mode, (long) bufferIndex * BUFFER_SIZE, BUFFER_SIZE);
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
        Queue queue = queueMap.computeIfAbsent(queueName, s -> new Queue());
        queue.messageAmount++;

        List<Block> blockList = queue.blockList;
        if (blockList.isEmpty()) blockList.add(new Block());

        Block block = blockList.get(blockList.size() - 1);
        block.messageAmount++;

        int messageBegin = 0;

        while (messageBegin < message.length) {
            int messageLength = message.length - messageBegin;
            MappedByteBuffer buffer = fetchBuffer(block, READ_WRITE);
            buffer.position((block.index % BLOCK_PER_BUFFER) * BLOCK_SIZE + queue.usedSlot * SLOT_SIZE);
            buffer.putInt(messageLength);
            int leftLength = (SLOT_PER_BLOCK - queue.usedSlot) * SLOT_SIZE - LENGTH_SIZE;
            if (leftLength > messageLength) {
                buffer.put(message, messageBegin, messageLength);
                queue.usedSlot += messageLength / SLOT_SIZE + 1;
            } else {
                buffer.put(message, 0, leftLength);
                queue.usedSlot = SLOT_PER_BLOCK;
            }
            if (messageBegin != 0) block.continuous = true;
            messageBegin = messageBegin + leftLength;
            if (queue.usedSlot == SLOT_PER_BLOCK) {
                block = new Block();
                blockList.add(block);
                queue.usedSlot = 0;
            }
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
        Queue queue = queueMap.get(queueName);
        if (queue == null || offset > queue.messageAmount) return Collections.emptyList();
        if (offset + num > queue.messageAmount) num = queue.messageAmount - offset;

        MappedByteBuffer buffer;
        List<byte[]> result = new ArrayList<>((int) num);
        byte[] continuousHead = null;

        List<Block> blockList = queue.blockList;
        for (Block block : blockList) {
            if (result.size() == num) return result;
            if (offset < block.messageAmount) {
                // 读取Block
                buffer = fetchBuffer(block, READ_ONLY);
                buffer.position((block.index % BLOCK_PER_BUFFER) * BLOCK_SIZE);
                buffer.get(blockBuffer, 0, BLOCK_SIZE);
                // 计算要跳过的消息数
                long skip = ((block.continuous && continuousHead == null) ? 1 : 0) + offset;
                // 清空偏移量，避免对后续造成影响
                offset = 0;
                // 检查每个Slot
                int slotIndex = 0;
                while (slotIndex < SLOT_PER_BLOCK) {
                    if (result.size() == num) return result;
                    // 计算消息起始位置
                    int slotBegin = slotIndex * SLOT_SIZE;
                    int length = 0;
                    for (int index = 0; index < LENGTH_SIZE; index++) {
                        length += (blockBuffer[slotBegin + (LENGTH_SIZE - index - 1)] & 0xFF) << (index * 8);
                    }
                    int messageBegin = slotBegin + LENGTH_SIZE;
                    int messageEnd = messageBegin + length;

                    if (skip > 0) {
                        // 需要跳过该条消息
                        skip--;
                    } else if (messageEnd < BLOCK_SIZE) {
                        // 消息在当前Block中结束
                        byte[] message;
                        if (continuousHead == null) {
                            // 消息在当前Block中开始
                            message = Arrays.copyOfRange(blockBuffer, messageBegin, messageEnd);
                        } else {
                            // 消息在之前的Block中开始
                            message = new byte[continuousHead.length + length];
                            System.arraycopy(continuousHead, 0, message, 0, continuousHead.length);
                            System.arraycopy(blockBuffer, messageBegin, message, continuousHead.length, length);
                            continuousHead = null;
                        }
                        result.add(message);
                    } else {
                        // 消息在之后的Block中结束，读取完该条消息后直接结束当前Block
                        if (continuousHead == null) {
                            // 消息在当前的Block中开始
                            continuousHead = Arrays.copyOfRange(blockBuffer, messageBegin, BLOCK_SIZE);
                        } else {
                            // 消息在之前的Block中开始
                            byte[] newContinuousHead = new byte[continuousHead.length + (BLOCK_SIZE - messageBegin)];
                            System.arraycopy(continuousHead, 0, newContinuousHead, 0, continuousHead.length);
                            System.arraycopy(blockBuffer, messageBegin, newContinuousHead, continuousHead.length, BLOCK_SIZE - messageBegin);
                            continuousHead = newContinuousHead;
                        }
                        break;
                    }
                    // 除非消息在之后的Block中结束，否则设置下一个Slot的序号
                    slotIndex = messageEnd / SLOT_SIZE + 1;
                }
            } else {
                // 跳过该Block并修改消息偏移量
                offset -= block.messageAmount;
            }
        }
        return result;
    }

    private class Queue {
        final List<Block> blockList;

        int messageAmount = 0;
        int usedSlot = 0;

        Queue() {
            this.blockList = new ArrayList<>(INITIAL_BLOCK_LIST_SIZE);
        }
    }

    private int blockIndex = 0;

    private class Block {
        final int index;

        int messageAmount = 0;
        boolean continuous = false;

        Block() {
            this.index = blockIndex++;
        }
    }

    private class Buffer {
        final MapMode mode;
        final MappedByteBuffer buffer;

        Buffer(MapMode mode, MappedByteBuffer buffer) {
            this.mode = mode;
            this.buffer = buffer;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        channel.close();
    }
}
