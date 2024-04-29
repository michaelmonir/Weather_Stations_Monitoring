package com.example.bitcask.Segments;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import lombok.Getter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class Segment {

    private int fileIndex;
    private HashMap<Long, Integer> map;
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    @Getter
    private int size;

    public Segment(int fileIndex) {
        this.size = 0;
        this.fileIndex = fileIndex;
        String fileName = getFileNameFromIndex();
        this.map = new HashMap<>();

        try {
            fileOutputStream = new FileOutputStream(fileName);
            fileInputStream = new FileInputStream(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int writeToFileAndGetOffset(byte[] data) {
        FileOperations.writeToFile(fileOutputStream, data);

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
        byte[] data = FileOperations.readFromFile(fileInputStream, offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }

    private String getFileNameFromIndex() {
        return Integer.toString(this.fileIndex);
    }
}
