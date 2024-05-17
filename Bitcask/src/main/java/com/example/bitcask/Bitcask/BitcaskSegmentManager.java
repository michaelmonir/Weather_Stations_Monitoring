package com.example.bitcask.Bitcask;

import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewRecovery.HintWriter;
import com.example.bitcask.NewRecovery.IndicesKeeper;
import com.example.bitcask.Segments.Segment;

import java.util.Iterator;
import java.util.Map;

public class BitcaskSegmentManager {

    private Bitcask bitcask;

    public BitcaskSegmentManager(Bitcask bitcask) {
        this.bitcask = bitcask;
    }

    public void handleExceedingMaxSize() {
        if (bitcask.getActiveSegment().getSize() >= Bitcask.getMaxSegmentSize()) {
            int segmentIndex = SegmentIncreamenter.getAndIncreament();

            Segment activeSegment = new Segment(segmentIndex);
            bitcask.setActiveSegment(activeSegment);

            new IndicesKeeper().addSegment(segmentIndex);
            this.writeSegmentToHintFile();

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

    private void writeSegmentToHintFile() {
        int sz = bitcask.getSegments().size();
        if (sz == 0) return;
        Segment segment = bitcask.getSegments().get(sz - 1);
        new HintWriter(segment).run();
    }
}
