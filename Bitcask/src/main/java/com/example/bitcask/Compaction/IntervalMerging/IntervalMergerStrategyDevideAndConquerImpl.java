package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Compaction.Interval;
import com.example.bitcask.Segments.Segment;

public class IntervalMergerStrategyDevideAndConquerImpl implements IntervalMergerStrategy {

    @Override
    public Segment mergeInterval(SegmentsWrapper segmentsWrapper, Interval interval) {
        return devidAndConquer(segmentsWrapper, interval.start, interval.end);
    }

    private Segment devidAndConquer(SegmentsWrapper segmentsWrapper, int st, int en) {
        if (st == en) return segmentsWrapper.segmentList.get(st);
        int mid = (st + en) / 2;
        Segment left = devidAndConquer(segmentsWrapper, st, mid);
        Segment right = devidAndConquer(segmentsWrapper, mid + 1, en);
        SegmentMerger segmentMerger = new SegmentMerger(left, right);
        return segmentMerger.mergeSegments();
    }
}
