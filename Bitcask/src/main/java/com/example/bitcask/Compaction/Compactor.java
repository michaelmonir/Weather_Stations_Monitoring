package com.example.bitcask.Compaction;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Compaction.IntervalMerging.IntervalMergerStrategy;
import com.example.bitcask.Compaction.IntervalMerging.SegmentsWrapper;
import com.example.bitcask.File.TextFileAtmoicUpdator;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.List;

public class Compactor {

    private CompactionStrategy compactionStrategy;
    private IntervalMergerStrategy intervalMergerStrategy;
    private List<Segment> segmentList;

    public Compactor(CompactionStrategy compactionStrategy, IntervalMergerStrategy intervalMergerStrategy) {
        this.compactionStrategy = compactionStrategy;
        this.intervalMergerStrategy = intervalMergerStrategy;
        this.segmentList = Bitcask.getBitcask().getSegments();
    }

    public void compact() {
        List<Interval> intervals = compactionStrategy.getCompactionIntervals(segmentList);
        List<Segment> segments = compactIntervals(intervals);

        Bitcask newBitcask = new Bitcask(segments);

        List<Integer> segmentIndeces = getSegmentIndeces(segments);
        TextFileAtmoicUpdator.writeList(segmentIndeces, "recoveryInformation.txt");
        FileCleaner.cleanExtraFiles(newBitcask);

        Bitcask.setBitcask(newBitcask);
    }

    private List<Segment> compactIntervals(List<Interval> intervals) {
        int sz = intervals.size();
        List<Segment> newSegmentsList = new ArrayList<>();

        for (Interval interval : intervals) {
            if (interval.end == sz-1) { // we don't compact the last segment
                newSegmentsList.add(segmentList.get(interval.end));
            } else {
                Segment newSegment = intervalMergerStrategy.mergeInterval(new SegmentsWrapper(this.segmentList), interval);
                newSegmentsList.add(newSegment);
            }
        }
        return newSegmentsList;
    }

    private List<Integer> getSegmentIndeces(List<Segment> segments) {
        List<Integer> integerList = new ArrayList<>();
        for (Segment segment : segments) {
            integerList.add(segment.getSegmentIndex());
        }
        return integerList;
    }
}
