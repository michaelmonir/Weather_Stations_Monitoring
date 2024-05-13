package com.example.bitcask.Compaction.CompactionStrategy;

import com.example.bitcask.Compaction.Interval;

import java.util.List;

public class CompactionStrategyImpl implements CompactionStrategy {
    @Override
    public List<Interval> getCompactionIntervals(List<Integer> segments) {
        int size = segments.size();
        if (size == 0)
            return List.of();
        else if (size == 1)
            return List.of(new Interval(0, 0));
        else
            return List.of(new Interval(0, size - 2), new Interval(size - 1, size - 1));
    }
}
