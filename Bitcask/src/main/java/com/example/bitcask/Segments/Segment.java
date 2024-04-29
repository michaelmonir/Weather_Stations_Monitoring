package com.example.bitcask.Segments;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.Message;
import lombok.Getter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Segment {

    private int fileIndex;
    private FileInputStream fileInputStream;
    private FileOutputStream fileOutputStream;
    @Getter
    private int size;

    public Segment(int fileIndex) {
        this.size = 0;
        this.fileIndex = fileIndex;
        String fileName = getFileNameFromIndex();

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

    public byte[] readFromFile(int offset) {
        return FileOperations.readFromFile(fileInputStream, offset);
    }

    private String getFileNameFromIndex() {
        return Integer.toString(this.fileIndex);
    }
}
