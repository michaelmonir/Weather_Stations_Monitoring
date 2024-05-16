package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.BitcaskSegmentManager;
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
        this.writeMergedFile();
        this.mergeWithBitcaskMap();
        this.doAtomicOperations();
    }

    private void setResultSegment() {
        this.resultSegment = new Segment(SegmentIncreamenter.getAndIncreament());
    }

    private void mergeWithBitcaskMap() {
        Bitcask bitcask = Bitcask.getBitcask();
        new BitcaskSegmentManager(bitcask).mergeWithMap(this.resultSegment.getMyMap());
    }

    private void writeMergedFile() {
        MergedFileWriter mergedFileWriter
                = new MergedFileWriter(this.mapGenerator.getMap(), this.resultSegment);
        mergedFileWriter.run();
    }

    private void doAtomicOperations() {
        AtomicMergeOperations atomicMergeOperations
                = new AtomicMergeOperations(this.resultSegment, this.mapGenerator.getLastSegmentIndex());
        atomicMergeOperations.deleteOldFilesAndSetSegment();
    }
}
