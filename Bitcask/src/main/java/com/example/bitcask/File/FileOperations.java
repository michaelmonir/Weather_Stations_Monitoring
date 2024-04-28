package com.example.bitcask.File;

import com.example.bitcask.Message.Message;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

@Service
public class FileOperations {

    String fileName = "log.bin";
    private FileOutputStream fileOutputStream;
    private FileInputStream fileInputStream;
    private int offset = 0;

    public FileOperations() {
        try {
            this.fileOutputStream = new FileOutputStream(this.fileName);
            this.fileInputStream = new FileInputStream(this.fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int writeToFileAndGetOffset(byte[] data) {
        try {
            fileOutputStream.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int ret = offset;
        offset += Message.MESSAGE_SIZE;
        return ret;
    }

    public byte[] readFromFile(int offset) {
        try {
            byte[] data = new byte[Message.MESSAGE_SIZE];
            fileInputStream.read(data, offset, Message.MESSAGE_SIZE);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void closeFile() {
        try {
            this.fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
