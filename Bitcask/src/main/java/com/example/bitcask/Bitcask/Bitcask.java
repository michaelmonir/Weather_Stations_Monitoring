package com.example.bitcask.Bitcask;

import com.example.bitcask.Exceptions.BiggerTimestampExistsException;
import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Recovery.RecoveryInformationUpdater;
import com.example.bitcask.Segments.Segment;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class Bitcask {

    private MyMap myMap;
    private static Bitcask simgletonBitcask;
    private final int maxSegmentSize = 100;
    private Segment segment;
    @Getter
    private List<Segment> segments;

    public Bitcask() {
        segments = new ArrayList<>();
        this.myMap = new MyMap();
        segment = new Segment(SegmentIncreamenter.getAndIncreament());
        segments.add(segment);
    }

    public Bitcask(List<Segment> segments) {
        this.segments = segments;
        this.segment = segments.get(segments.size() - 1);
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

    public void write(Message message) {
        this.handleExceedingMaxSize();

        try {
            MapEntry mapEntry = this.segment.write(message);
            this.myMap.put(message.getStation_id(), mapEntry);
        } catch (BiggerTimestampExistsException e) {
        }
    }

    public Message read(Long stationId) {
        MapEntry mapEntry = myMap.get(stationId);
        int fileIndex = mapEntry.fileIndex, offset = mapEntry.offset;

        String fileName = FileNameGetter.getFileName(fileIndex);
        byte[] bytes = BinaryFileOperations.readFromFile(fileName, offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(bytes);
        return byteToMessageConverter.convert();
    }

    private void handleExceedingMaxSize() {
        if (this.segment.getSize() >= maxSegmentSize) {
            int segmentIndex = SegmentIncreamenter.getAndIncreament();

            segment = new Segment(segmentIndex);
            RecoveryInformationUpdater recoveryInformationUpdater = new RecoveryInformationUpdater();
            recoveryInformationUpdater.addSegment(segmentIndex);

            segments.add(segment);
        }
    }
}
