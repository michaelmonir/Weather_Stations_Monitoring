package com.example.bitcask.Hashmap;

public class MapEntry {
    public int fileIndex;
    public int offset;
    public Long timestamp;

    public MapEntry(Long timestamp, int fileIndex, int offset) {
        this.fileIndex = fileIndex;
        this.offset = offset;
        this.timestamp = timestamp;
    }
}
