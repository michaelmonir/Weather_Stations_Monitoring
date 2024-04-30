package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Compaction.Interval;
import com.example.bitcask.Segments.Segment;

public class IntervalMergerStrategyImpl implements IntervalMergerStrategy {

    @Override
    public Segment mergeInterval(SegmentsWrapper segmentsWrapper, Interval interval) {
        Segment oldSegment = new Segment(-1);
        for (int i = interval.start; i <= interval.end; i++) {
            Segment segment = segmentsWrapper.segmentList.get(i);

            SegmentMerger segmentMerger = new SegmentMerger(oldSegment, segment);
            oldSegment = segmentMerger.mergeSegments();
        }
        return oldSegment;
    }
}
