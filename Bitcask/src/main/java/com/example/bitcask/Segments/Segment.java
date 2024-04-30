package com.example.bitcask.Segments;

import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import lombok.Getter;

import java.util.HashMap;
import java.util.Iterator;

public class Segment {

    private String fileName;
    @Getter
    private HashMap<Long, Integer> map;
    @Getter
    private int size;

    public Segment(int fileIndex) {
        this.size = 0;
        fileName = FileNameGetter.getFileName(fileIndex);
        this.map = new HashMap<>();
    }

    public Segment(String fileName, HashMap<Long, Integer> map, int size) {
        this.fileName = fileName;
        this.map = map;
        this.size = size;
    }

    public int getSegmentIndex() {
        return Integer.parseInt(this.fileName);
    }

    public int writeToFileAndGetOffset(byte[] data) {
        BinaryFileOperations.writeToFile(this.fileName, data);

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
        byte[] data = BinaryFileOperations.readFromFile(this.fileName, offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }
}
