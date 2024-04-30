package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Segments.Segment;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class HashMapToSegmentConverter {

    private Segment segment;
    private HashMap<Long, Integer> map;

    public HashMapToSegmentConverter(HashMap<Long, Integer> hashMap) {
        this.map = hashMap;
        this.segment = new Segment(SegmentIncreamenter.getAndIncreament());
    }

    public Segment convert() {
        map.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(entry -> {

                    segment.writeToFileAndGetOffset(entry.getKey());
                });
        return segment;
    }
}
