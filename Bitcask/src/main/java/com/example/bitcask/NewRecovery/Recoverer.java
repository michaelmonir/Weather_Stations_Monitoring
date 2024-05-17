package com.example.bitcask.NewRecovery;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.File.TextFileOperations;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.List;

public class Recoverer {

    public Bitcask recover() {
        List<Integer> segmentIndices = new IndicesKeeper().recover();
        List<Segment> segments = this.getSegments(segmentIndices);

        return new Bitcask(segments);
    }

    private List<Segment> getSegments(List<Integer> segmentIndices) {
        List<Segment> segments = new ArrayList<>();
        for (int index : segmentIndices) {
            Segment segment = this.pickAndrunRecoveryChoice(index);
            segments.add(segment);
        }
        return segments;
    }

    private Segment pickAndrunRecoveryChoice(int index) {
        if (TextFileOperations.fileExists(FileNameGetter.getHintFileName(index))) {
            return new SegmentWithHintRecoverer(index).recover();
        } else {
            return new SegmentWithoutHintRecoverer(index).recover();
        }
    }
}
