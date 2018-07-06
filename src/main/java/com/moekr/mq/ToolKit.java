package com.moekr.mq;

import sun.nio.ch.FileChannelImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

@SuppressWarnings("JavaReflectionMemberAccess")
abstract class ToolKit {
    private static final Method UNMAP_METHOD;

    static {
        try {
            UNMAP_METHOD = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            UNMAP_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    static void unmap(MappedByteBuffer buffer) {
        try {
            UNMAP_METHOD.invoke(null, buffer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
