package com.moekr.mq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class QueueStore extends io.openmessaging.QueueStore {
    private static Collection<byte[]> EMPTY = new ArrayList<>();

    private final Map<String, List<byte[]>> queueMap = new ConcurrentHashMap<>();

    public void put(String queueName, byte[] message) {
        List<byte[]> queue = queueMap.computeIfAbsent(queueName, s -> new ArrayList<>());
        synchronized (queue) {
            queue.add(message);
        }
    }
    public Collection<byte[]> get(String queueName, long offset, long num) {
        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        List<byte[]> queue = queueMap.get(queueName);
        synchronized (queue) {
            return queue.subList((int) offset, offset + num > queue.size() ? queue.size() : (int) (offset + num));
        }
    }
}
