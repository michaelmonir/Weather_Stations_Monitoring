package com.example.bitcask.Bitcask;

import com.example.bitcask.Errors.NoMessageWithThisIdException;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import com.example.bitcask.Segments.Segment;

import java.util.ArrayList;
import java.util.List;

public class Bitcask {

    private final int maxSegmentSize = 200;
    private Segment segment;
    private List<Segment> segments;
    private int numOfSegments;

    public Bitcask() {
        numOfSegments = 1;
        segments = new ArrayList<>();
        segment = new Segment(numOfSegments);
        segments.add(segment);
    }

    public Bitcask(List<Segment> segments) {
        this.segments = segments;
        this.segment = segments.get(segments.size() - 1);
        this.numOfSegments = segments.size();
    }

    public void write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();

        this.segment.write(message.getStation_id(), data);

        this.handleExceedingMaxSize();
    }

    public Message read(Long stationId) {
        Segment segment = getSegmentOfWrite(stationId);
        return segment.read(stationId);
    }

    private Segment getSegmentOfWrite(long stationId) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (segments.get(i).hasStation(stationId))
                return segments.get(i);
        }
        throw new NoMessageWithThisIdException();
    }

    private void handleExceedingMaxSize() {
        if (this.segment.getSize() > maxSegmentSize) {
            segment = new Segment(++numOfSegments);
            segments.add(segment);
        }
    }
}
