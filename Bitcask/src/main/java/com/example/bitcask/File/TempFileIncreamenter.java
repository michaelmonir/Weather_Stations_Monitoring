package com.example.bitcask.File;

import java.util.concurrent.atomic.AtomicInteger;

public class TempFileIncreamenter {

    static AtomicInteger tempFileIndex = new AtomicInteger(0);

    public static int getTempFileIndex() {
        return tempFileIndex.getAndIncrement();
    }
}
