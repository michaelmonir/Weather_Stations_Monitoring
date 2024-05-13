package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Segments.Segment;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapGenerator {

    private List<Segment> oldSegments = new ArrayList<>();
    @Getter
    private int lastSegmentIndex;
    private MyMap oldSegmentsMap = new MyMap();

    public MapGenerator() {
        this.setOldSegments();
        this.calculateOldSegmentsMap();
    }

    public MyMap getMap() {
        return oldSegmentsMap;
    }

    private void setOldSegments() {
        this.lastSegmentIndex = Bitcask.getBitcask().getSegments().size()-1;
        for (int i = 0; i < lastSegmentIndex; i++)
            oldSegments.add(Bitcask.getBitcask().getSegments().get(i));
    }

    private void calculateOldSegmentsMap() {
        for (Segment segment : oldSegments) {
            MyMap segmentMap = segment.getMyMap();
            Iterator<Map.Entry<Long, MapEntry>> it = segmentMap.getIterator();
            while (it.hasNext()) {
                Map.Entry<Long, MapEntry> entry = it.next();
                oldSegmentsMap.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
