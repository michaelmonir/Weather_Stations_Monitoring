package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.BitcaskLocks;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.File.TextFileAtmoicUpdator;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.List;

public class AtomicMergeOperations {

    private Segment resultSegment;
    private int lastSegmentIndex;

    public AtomicMergeOperations(Segment resultSegment, int lastSegmentIndex) {
        this.resultSegment = resultSegment;
        this.lastSegmentIndex = lastSegmentIndex;
    }

    public void deleteOldFilesAndSetSegment() {
        BitcaskLocks.lockWrite();

        this.setSegmentsList();
        this.updateUsedSegmentsFile();
        FileCleaner.cleanExtraFiles(Bitcask.getBitcask());
        BitcaskLocks.unlockWrite();
    }

    private void setSegmentsList() {
        List<Segment> list = new ArrayList<>();
        list.add(this.resultSegment);

        List<Segment> bitcaskCurrentSegments = Bitcask.getBitcask().getSegments();
        for (int i = lastSegmentIndex; i < bitcaskCurrentSegments.size(); i++)
            list.add(bitcaskCurrentSegments.get(i));

        Bitcask.getBitcask().setSegments(list);
    }

    private void updateUsedSegmentsFile() {
        List<Integer> segmentIndeces = new ArrayList<>();
        for (Segment segment : Bitcask.getBitcask().getSegments())
            segmentIndeces.add(segment.getSegmentIndex());

        TextFileAtmoicUpdator.writeList(segmentIndeces, FileNameGetter.getIndexesFileName());
    }
}
