package com.example.bitcask.Bitcask;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BitcaskLocks {

    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final static Lock readLock = lock.readLock();
    private final static Lock writeLock = lock.writeLock();

    public static void lockWrite() {
        writeLock.lock();
    }

    public static void unlockWrite() {
        writeLock.unlock();
    }

    public static void lockRead() {
        readLock.lock();
    }

    public static void unlockRead() {
        readLock.unlock();
    }
}
