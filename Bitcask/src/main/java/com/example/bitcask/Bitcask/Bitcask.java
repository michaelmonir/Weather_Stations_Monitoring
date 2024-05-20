package com.example.bitcask.Bitcask;

import com.example.bitcask.Exceptions.BiggerTimestampExistsException;
import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Converters.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewRecovery.IndicesKeeper;
import com.example.bitcask.Segments.Segment;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class Bitcask {

    @Getter
    private MyMap myMap;
    private static Bitcask simgletonBitcask;
    @Setter
    @Getter
    private static int maxSegmentSize = 1;
    @Setter
    @Getter
    private Segment activeSegment;
    @Setter
    public static int maxNumOfSegments = 1000000;
    @Getter
    @Setter
    private List<Segment> segments;

    public Bitcask() {
        segments = new ArrayList<>();
        this.myMap = new MyMap();
        int segmentIndex = SegmentIncreamenter.getAndIncreament();
        activeSegment = new Segment(segmentIndex);
        segments.add(activeSegment);
        new IndicesKeeper().addSegment(segmentIndex);
    }

    public Bitcask(List<Segment> segments) {
        this.segments = segments;
        this.activeSegment = segments.get(segments.size() - 1);
        this.myMap = new MyMap();
        for (Segment segment : this.segments)
            new BitcaskSegmentManager(this).mergeWithMap(segment.getMyMap());
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

    public synchronized void write(Message message) {
        new BitcaskSegmentManager(this).handleExceedingMaxSize();
        new BitcaskSegmentManager(this).handleMaxNumOfSegments();

        try {
            MapEntry mapEntry = this.activeSegment.write(message);
            this.myMap.put(message.getStation_id(), mapEntry);
        } catch (BiggerTimestampExistsException e) {
        }
    }

    public Message read(Long stationId) {
        MapEntry mapEntry = myMap.get(stationId); // get first to make sure it is already present

        BitcaskLocks.lockRead();
        int fileIndex = mapEntry.fileIndex, offset = mapEntry.offset;

        String fileName = FileNameGetter.getFileName(fileIndex);
        byte[] bytes = BinaryFileOperations.readFromFile(fileName, offset);

        BitcaskLocks.unlockRead();
        return new ByteToMessageConverter(bytes).convert();
    }
}
