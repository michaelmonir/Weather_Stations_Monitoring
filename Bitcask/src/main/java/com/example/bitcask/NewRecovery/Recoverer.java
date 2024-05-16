package com.example.bitcask.NewRecovery;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.List;

public class Recoverer {

    public Bitcask recover() {
        List<Integer> segmentIndices = new SegmentIndicesRecoverer().recover();
        List<Segment> segments = this.getSegments(segmentIndices);

        return new Bitcask(segments);
    }

    private List<Segment> getSegments(List<Integer> segmentIndices) {
        List<Segment> segments = new ArrayList<>();
        for (int index : segmentIndices) {
            Segment segment = new SegmentRecoverer(index).recover();
            segments.add(segment);
        }
        return segments;
    }
}
