package com.example.bitcask.Bitcask;

import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import com.example.bitcask.Segments.Segment;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

public class Bitcask {

    private HashMap<Long, Integer> map;
    private final int maxSegmentSize = 3;
    private Segment segment;
    private List<Segment> segments;
    private int numOfSegments;

    public Bitcask() {
        map = new HashMap<>();
        numOfSegments = 1;
        segment = new Segment(numOfSegments);
        segments.add(segment);
    }

    public synchronized void write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();

        int offset = segment.writeToFileAndGetOffset(data);
        map.put(message.getStation_id(), offset);
        this.handleExceedingMaxSize();
    }

    public Message read(Long station_id) {
        int offset = map.get(station_id);
        byte[] data = segment.readFromFile(offset);
        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }

    private void handleExceedingMaxSize() {
        if (this.segment.getSize() == maxSegmentSize) {
            segment = new Segment(++numOfSegments);
            segments.add(segment);
        }
    }
}
