package com.example.bitcask.Bitcask;

import com.example.bitcask.Segments.Segment;

import java.util.List;

public class SegmentIncreamenter {
    private static int segmentNumber = 1;

    public static int getAndIncreament() {
        return segmentNumber++;
    }

    public static void recover(List<Segment> segmentList) {
        for (Segment segment : segmentList)
            segmentNumber = Math.max(segmentNumber, segment.getSegmentIndex());
    }
}
