package com.example.bitcask.Segments;

import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import lombok.Getter;

import java.util.HashMap;

public class Segment {

    @Getter
    private int segmentIndex;
    @Getter
    private HashMap<Long, Integer> map;
    @Getter
    private int size;

    public Segment(int segmentIndex) {
        this.size = 0;
        this.segmentIndex = segmentIndex;
        this.map = new HashMap<>();
    }

    public Segment(int segmentIndex, HashMap<Long, Integer> map, int size) {
        this.segmentIndex = segmentIndex;
        this.map = map;
        this.size = size;
    }

    public int writeToFileAndGetOffset(byte[] data) {
        BinaryFileOperations.writeToFile(FileNameGetter.getFileName(this.segmentIndex), data);

        int ret = size;
        size += Message.MESSAGE_SIZE;
        return ret;
    }

    public boolean hasStation(long stationId) {
        return this.map.containsKey(stationId);
    }

    public void write(long stationId, byte[] data) {
        int offset = this.writeToFileAndGetOffset(data);
        map.put(stationId, offset);
    }

    public Message read(Long stationId) {
        int offset = map.get(stationId);
        byte[] data = BinaryFileOperations.readFromFile(FileNameGetter.getFileName(this.segmentIndex), offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }
}
