package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import com.example.bitcask.Segments.Segment;

import java.util.HashSet;
import java.util.Map;

public class SegmentMerger {

    private Segment segment1, segment2;
    private HashSet<Long> set;
    private Segment resultSegment;

    public SegmentMerger(Segment segment1, Segment segment2) {
        this.segment1 = segment1;
        this.segment2 = segment2;
        set = new HashSet<>();
        int segmentIndex = SegmentIncreamenter.getAndIncreament();
        this.resultSegment = new Segment(segmentIndex);
    }

    public Segment mergeSegments() {
        segmentIteration(segment2);
        segmentIteration(segment1);
        return resultSegment;
    }

    private void segmentIteration(Segment segment) {
        segment.getMap().entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(entry -> {
                    if (set.contains(entry.getKey())) {
                        return;
                    }
                    set.add(entry.getKey());
                    Long stationId = entry.getKey();
                    this.writeMessageToResultSegment(segment, stationId);
                });
    }

    private void writeMessageToResultSegment(Segment segment, Long stationId) {
        Message message = segment.read(stationId);

        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();

        resultSegment.write(stationId, data);
    }
}
