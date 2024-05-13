package com.example.bitcask.Hashmap;

import com.example.bitcask.Exceptions.BiggerTimestampExistsException;
import com.example.bitcask.Exceptions.IdDoesNotExistException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyMap {

    private ConcurrentHashMap<Long, MapEntry> hashMap;

    public MyMap() {
        this.hashMap = new ConcurrentHashMap<>();
    }

    public MapEntry get(long key) {
        if (!hashMap.containsKey(key)) throw new IdDoesNotExistException();
        return hashMap.get(key);
    }

    public Iterator<Map.Entry<Long, MapEntry>> getIterator() {
        return hashMap.entrySet().iterator();
    }

    public boolean containsKey(long key) {
        return hashMap.containsKey(key);
    }

    public MapEntry put(long key, Long timestamp, int fileIndex, int offset) {
        if (hashMap.containsKey(key) && hashMap.get(key).timestamp > timestamp)
            throw new BiggerTimestampExistsException();
        MapEntry entry = new MapEntry(timestamp, fileIndex, offset);
        hashMap.put(key, entry);
        return entry;
    }

    public void put(Long stationId, MapEntry mapEntry) {
        this.put(stationId, mapEntry.timestamp, mapEntry.fileIndex, mapEntry.offset);
    }
}
