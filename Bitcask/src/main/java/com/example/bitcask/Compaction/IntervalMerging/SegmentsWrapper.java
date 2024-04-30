package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Segments.Segment;

import java.util.List;

public class SegmentsWrapper {

    public List<Segment> segmentList;

    public SegmentsWrapper(List<Segment> segmentList) {
        this.segmentList = segmentList;
    }
}
