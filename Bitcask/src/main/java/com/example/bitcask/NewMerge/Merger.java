package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.BitcaskLocks;
import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Compaction.FileCleaner;
import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.File.TextFileAtmoicUpdator;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Merger {

    private List<Segment> oldSegments = new ArrayList<>();
    private MyMap oldSegmentsMap = new MyMap();
    private int lastSegmentIndex;
    private Segment resultSegment;

    public void run() {
        if (Bitcask.getBitcask().getSegments().size() < 2)
            return;
        this.setOldSegments();
        this.mergeIntervalsAndGetOthers();
    }

    private void setOldSegments() {
        lastSegmentIndex = Bitcask.getBitcask().getSegments().size()-1;
        for (int i = 0; i < lastSegmentIndex; i++)
            oldSegments.add(Bitcask.getBitcask().getSegments().get(i));
    }

    private void mergeIntervalsAndGetOthers() {
        this.resultSegment = new Segment(SegmentIncreamenter.getAndIncreament());

        this.calculateOldSegmentsMap();
        writeMapToFile();
        this.writeHint();

        Bitcask.getBitcask().mergeWithMap(this.oldSegmentsMap);

        this.deleteOldFilesAndSetSegment();
    }

    private void calculateOldSegmentsMap() {
        for (Segment segment : oldSegments) {
            MyMap segmentMap = segment.getMyMap();
            Iterator<Map.Entry<Long, MapEntry>> it = segmentMap.getIterator();
            while (it.hasNext()) {
                Map.Entry<Long, MapEntry> entry = it.next();
                oldSegmentsMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void writeHint() {
        // TODO
    }

    private void deleteOldFilesAndSetSegment() {
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
        for (int i = lastSegmentIndex; i < bitcaskCurrentSegments.size(); i++) {
            list.add(bitcaskCurrentSegments.get(i));
        }

        Bitcask.getBitcask().setSegments(list);
    }

    private void updateUsedSegmentsFile() {
        List<Integer> segmentIndeces = new ArrayList<>();
        for (Segment segment : Bitcask.getBitcask().getSegments())
            segmentIndeces.add(segment.getSegmentIndex());

        TextFileAtmoicUpdator.writeList(segmentIndeces, FileNameGetter.getIndexesFileName());
    }

    private void writeMapToFile() {
        Iterator<Map.Entry<Long, MapEntry>> it = oldSegmentsMap.getIterator();
        while (it.hasNext()) {
            Map.Entry<Long, MapEntry> entry = it.next();

            MapEntry mapEntry = entry.getValue();
            String fileName = FileNameGetter.getFileName(mapEntry.fileIndex);

            byte[] bytes = BinaryFileOperations.readFromFile(fileName, mapEntry.offset);
            this.resultSegment.write(entry.getKey(), mapEntry.timestamp, bytes);
        }
    }
}
