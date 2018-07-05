package com.moekr.mq;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.moekr.mq.Constants.DATA_DIR;
import static com.moekr.mq.Constants.FILE_AMOUNT;

public class QueueStore extends io.openmessaging.QueueStore {
    private final QueueRegion[] regions = new QueueRegion[FILE_AMOUNT];

    public QueueStore() throws IOException {
        File dir = new File(DATA_DIR);
        if (!((dir.exists() && dir.isDirectory()) || dir.mkdirs())) {
            throw new IOException("Fail to create data dir!");
        }
        for (int index = 0; index < FILE_AMOUNT; index++) {
            regions[index] = new QueueRegion(index);
        }
    }

    @Override
    public void put(String queueName, byte[] message) {
        int index = Math.abs(queueName.hashCode()) % FILE_AMOUNT;
        regions[index].put(queueName, message);
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        int index = Math.abs(queueName.hashCode()) % FILE_AMOUNT;
        return regions[index].get(queueName, offset, num);
    }
}
