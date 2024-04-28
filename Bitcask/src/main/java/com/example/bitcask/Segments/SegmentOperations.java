package com.example.bitcask.Segments;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.Message;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SegmentOperations {

    private String fileName;
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    private int offset;

    public SegmentOperations(String fileName) {
        this.offset = offset;
        this.fileName = fileName;
        try {
            fileOutputStream = new FileOutputStream(fileName);
            fileInputStream = new FileInputStream(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int writeToFileAndGetOffset(byte[] data) {
        FileOperations.writeToFile(fileOutputStream, data);

        int ret = offset;
        offset += Message.MESSAGE_SIZE;
        return ret;
    }

    public byte[] readFromFile(int offset) {
        return FileOperations.readFromFile(fileInputStream, offset);
    }
}
