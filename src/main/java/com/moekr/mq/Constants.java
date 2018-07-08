package com.moekr.mq;

abstract class Constants {
    static final String DATA_DIR = System.getProperty("data.dir", "/alidata1/race2018/data/");
    static final int FILE_AMOUNT = 64;
    static final int SLOT_SIZE = 64;
    static final int SLOT_PER_BLOCK = 16;
    static final int BLOCK_SIZE = SLOT_SIZE * SLOT_PER_BLOCK;
    static final int BLOCK_PER_BUFFER = 4096;
    static final int BUFFER_SIZE = BLOCK_SIZE * BLOCK_PER_BUFFER;
    static final int MAX_LOADED_BUFFER = 8;
    static final int LENGTH_SIZE = 4;

    static final int INITIAL_QUEUE_MAP_SIZE = 16384;
    static final int INITIAL_BLOCK_LIST_SIZE = 144;
}
