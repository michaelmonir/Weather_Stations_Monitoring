package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Segments.Segment;

public class Merger {

    private Segment resultSegment;
    private MapGenerator mapGenerator;

    public void run() {
        if (Bitcask.getBitcask().getSegments().size() < 2)
            return;
        this.mapGenerator = new MapGenerator();

        this.setResultSegment();
        this.mergeIntervalsAndGetOthers();
    }

    private void mergeIntervalsAndGetOthers() {
        MergedFilesWriter mergedFilesWriter
                = new MergedFilesWriter(this.mapGenerator.getMap(), this.resultSegment);
        mergedFilesWriter.run();

        this.mergeWithBitcaskMap();

        AtomicMergeOperations atomicMergeOperations
                = new AtomicMergeOperations(this.resultSegment, this.mapGenerator.getLastSegmentIndex());
        atomicMergeOperations.deleteOldFilesAndSetSegment();
    }

    private void setResultSegment() {
        this.resultSegment = new Segment(SegmentIncreamenter.getAndIncreament());
    }

    private void mergeWithBitcaskMap() {
        Bitcask.getBitcask().mergeWithMap(this.resultSegment.getMyMap());
    }
}
