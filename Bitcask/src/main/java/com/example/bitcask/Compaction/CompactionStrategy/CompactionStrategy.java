package com.example.bitcask.Compaction.CompactionStrategy;

import com.example.bitcask.Compaction.Interval;
import com.example.bitcask.Segments.Segment;

import java.util.List;

public interface CompactionStrategy {
    List<Interval> getCompactionIntervals(List<Integer> segments);
}
