package com.example.bitcask.Segments;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import lombok.Getter;

import java.util.HashMap;

public class Segment {

    private String fileName;
    private HashMap<Long, Integer> map;
    @Getter
    private int size;

    public Segment(int fileIndex) {
        this.size = 0;
        fileName = getFileNameFromIndex(fileIndex);
        this.map = new HashMap<>();
    }

    public int writeToFileAndGetOffset(byte[] data) {
        FileOperations.writeToFile(this.fileName, data);

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
        byte[] data = FileOperations.readFromFile(this.fileName, offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }

    private String getFileNameFromIndex(int fileIndex) {
        return Integer.toString(fileIndex);
    }
}
