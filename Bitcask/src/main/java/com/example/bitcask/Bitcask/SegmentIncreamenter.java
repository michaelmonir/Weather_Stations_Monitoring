package com.example.bitcask.Bitcask;

import com.example.bitcask.Segments.Segment;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentIncreamenter {
    private static AtomicInteger atomicSegmentNumber = new AtomicInteger(1);

    public static int getAndIncreament() {
        return atomicSegmentNumber.getAndIncrement();
    }

    public static void recover(List<Segment> segmentList) {
        int segmentNumber = 1;
        for (Segment segment : segmentList)
            segmentNumber = Math.max(segmentNumber, segment.getSegmentIndex());
        atomicSegmentNumber.set(segmentNumber);
    }
}
