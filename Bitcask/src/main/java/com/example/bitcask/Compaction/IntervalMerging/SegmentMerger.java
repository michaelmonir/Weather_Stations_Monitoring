package com.example.bitcask.Compaction.IntervalMerging;

import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import com.example.bitcask.Segments.Segment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

public class SegmentMerger {

    private Segment segment1, segment2;
    private Segment resultSegment;

    public SegmentMerger(Segment segment1, Segment segment2) {
        this.segment1 = segment1;
        this.segment2 = segment2;
        int segmentIndex = SegmentIncreamenter.getAndIncreament();
        this.resultSegment = new Segment(segmentIndex);
    }

    public Segment mergeSegments() {
        iterate(segment1, this::IterationSegment1);
        iterate(segment2, this::IterationSegment2);
        return resultSegment;
    }

    private void iterate(Segment segment, Consumer<Long> function) {
        segment.getMap().entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(entry -> function.accept(entry.getKey()));
    }

    private void IterationSegment1(Long stationId) {
        if (segment2.hasStation(stationId))
            return;
        this.writeMessageToResultSegment(segment1, stationId);
    }

    private void IterationSegment2(Long stationId) {
        this.writeMessageToResultSegment(segment2, stationId);
    }

    private void writeMessageToResultSegment(Segment segment, Long stationId) {
        Message message = segment.read(stationId);

        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();

        resultSegment.write(stationId, data);
    }
}
