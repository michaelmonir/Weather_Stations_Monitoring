package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Compaction.Interval;
import com.example.bitcask.Segments.Segment;

import java.util.HashMap;

public interface IntervalMergerStrategy {
    Segment mergeInterval(SegmentsWrapper segmentsWrapper, Interval interval);
}
