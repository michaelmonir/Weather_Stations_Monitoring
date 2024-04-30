package com.example.bitcask.Compaction;

import com.example.bitcask.Segments.Segment;

import java.util.List;

public interface CompactionStrategy {
    List<Interval> getCompactionIntervals(List<Segment> segments);
}
