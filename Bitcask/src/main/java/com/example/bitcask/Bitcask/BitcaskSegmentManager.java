package com.example.bitcask.Bitcask;

import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewRecovery.RecoveryInformationUpdater;
import com.example.bitcask.Segments.Segment;

import java.util.Iterator;
import java.util.Map;

public class BitcaskSegmentManager {

    private Bitcask bitcask;

    public BitcaskSegmentManager(Bitcask bitcask) {
        this.bitcask = bitcask;
    }

    public void handleExceedingMaxSize() {
        if (bitcask.getActiveSegment().getSize() >= Bitcask.maxSegmentSize) {
            int segmentIndex = SegmentIncreamenter.getAndIncreament();

            Segment activeSegment = new Segment(segmentIndex);
            bitcask.setActiveSegment(activeSegment);

            new RecoveryInformationUpdater().addSegment(segmentIndex);

            bitcask.getSegments().add(activeSegment);
        }
    }

    public void handleMaxNumOfSegments() {
        if (bitcask.getSegments().size() >= Bitcask.maxNumOfSegments) {
            MergeScheduler.resume();
        }
    }

    public void mergeWithMap(MyMap otherMap) {
        Iterator it = otherMap.getIterator();
        while (it.hasNext()) {
            Map.Entry<Long, MapEntry> entry = (Map.Entry<Long, MapEntry>) it.next();
            bitcask.getMyMap().put(entry.getKey(), entry.getValue());
        }
    }
}
