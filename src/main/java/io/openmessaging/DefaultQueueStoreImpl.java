package io.openmessaging;

import java.io.IOException;
import java.util.Collection;

public class DefaultQueueStoreImpl extends QueueStore {
    private final com.moekr.mq.QueueStore store;

    public DefaultQueueStoreImpl() throws IOException {
        this.store = new com.moekr.mq.QueueStore();
    }

    @Override
    public void put(String queueName, byte[] message) {
        this.store.put(queueName, message);
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        return this.store.get(queueName, offset, num);
    }
}
