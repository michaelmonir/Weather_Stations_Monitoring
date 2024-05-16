package com.example.bitcask.Bitcask;

import com.example.bitcask.Exceptions.BiggerTimestampExistsException;
import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewRecovery.RecoveryInformationUpdater;
import com.example.bitcask.Segments.Segment;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Bitcask {

    private MyMap myMap;
    private static Bitcask simgletonBitcask;
    private static int maxSegmentSize = 1;
    private Segment segment;
    private static int maxNumOfSegments = 1000000;
    @Getter
    @Setter
    private List<Segment> segments;
    public static AtomicInteger bitcaskDebugger = new AtomicInteger(0);
    public static AtomicInteger getBitcaskDebuggerStatus = new AtomicInteger(0);

    public Bitcask() {
        segments = new ArrayList<>();
        this.myMap = new MyMap();
        int segmentIndex = SegmentIncreamenter.getAndIncreament();
        segment = new Segment(segmentIndex);
        segments.add(segment);
        new RecoveryInformationUpdater().addSegment(segmentIndex);
    }

    public Bitcask(List<Segment> segments) {
        this.segments = segments;
        this.segment = segments.get(segments.size() - 1);
        this.myMap = new MyMap();
        for (Segment segment : this.segments)
            this.mergeWithMap(segment.getMyMap());
    }

    public static Bitcask getBitcask() {
        if (simgletonBitcask == null) {
            simgletonBitcask = new Bitcask();
        }
        return simgletonBitcask;
    }

    public static Bitcask setBitcask(Bitcask bitcask) {
        simgletonBitcask = bitcask;
        return simgletonBitcask;
    }

    public static void setMaxSegmentSize(int maxSegmentSize) {
        Bitcask.maxSegmentSize = maxSegmentSize;
    }

    public static void setMaxNumOfSegments(int maxNumberOfSegments) {
        Bitcask.maxNumOfSegments = maxNumberOfSegments;
    }

    public synchronized void write(Message message) {
        BitcaskLocks.lockRead();

        this.handleExceedingMaxSize();
        this.handleMaxNumOfSegments();

        try {
            MapEntry mapEntry = this.segment.write(message);
            this.myMap.put(message.getStation_id(), mapEntry);
        } catch (BiggerTimestampExistsException e) {
        }
        BitcaskLocks.unlockRead();
    }

    public Message read(Long stationId) {
        MapEntry mapEntry = myMap.get(stationId); // get first to make sure it is already present

        BitcaskLocks.lockRead();
        int fileIndex = mapEntry.fileIndex, offset = mapEntry.offset;

        String fileName = FileNameGetter.getFileName(fileIndex);
        byte[] bytes = BinaryFileOperations.readFromFile(fileName, offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(bytes);

        BitcaskLocks.unlockRead();
        return byteToMessageConverter.convert();
    }

    public void mergeWithMap(MyMap otherMap) {
        Iterator it = otherMap.getIterator();
        while (it.hasNext()) {
            Map.Entry<Long, MapEntry> entry = (Map.Entry<Long, MapEntry>) it.next();
            this.myMap.put(entry.getKey(), entry.getValue());
        }
    }

    private void handleExceedingMaxSize() {
        if (this.segment.getSize() >= maxSegmentSize) {
            int segmentIndex = SegmentIncreamenter.getAndIncreament();

            segment = new Segment(segmentIndex);
            new RecoveryInformationUpdater().addSegment(segmentIndex);

            segments.add(segment);
        }
    }

    private void handleMaxNumOfSegments() {
        if (this.segments.size() >= maxNumOfSegments) {
            MergeScheduler.resume();
        }
    }
}
