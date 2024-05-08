package com.example.bitcask.Hashmap;

import java.util.HashMap;

public class MyMap {

    private HashMap<Long, FileWithOffset> hashMap;

    public MyMap() {
        this.hashMap = new HashMap<>();
    }

    public FileWithOffset get(long key) {
        return hashMap.get(key);
    }

    public void put(long key, int fileIndex, int offset) {
        hashMap.put(key, new FileWithOffset(fileIndex, offset));
    }
}
