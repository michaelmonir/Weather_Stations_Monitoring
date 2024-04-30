package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Compaction.Interval;
import com.example.bitcask.Segments.Segment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IntervalMergerStrategyImpl implements IntervalMergerStrategy {

    @Override
    public HashMap<Long, Integer> mergeInterval(SegmentsWrapper segmentsWrapper, Interval interval) {
        HashMap<Long, Integer> map = new HashMap<>();
        for (int i = interval.start; i <= interval.end; i++)
            mergeWithSegmentMap(segmentsWrapper.segmentList.get(i), new MapWrapper(map));
        return map;
    }

    private void mergeWithSegmentMap(Segment segment, MapWrapper mapWrapper) {
        Iterator<HashMap.Entry<Long, Integer>> iterator = segment.getIterator();
        while (iterator.hasNext()) {
            HashMap.Entry<Long, Integer> entry = iterator.next();
            mapWrapper.map.put(entry.getKey(), entry.getValue());
        }
    }

    class MapWrapper {
        public HashMap<Long, Integer> map;

        public MapWrapper(HashMap<Long, Integer> map) {
            this.map = map;
        }
    }
}
