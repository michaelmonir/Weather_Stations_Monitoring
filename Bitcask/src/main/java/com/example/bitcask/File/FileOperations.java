package com.example.bitcask.File;

import org.springframework.stereotype.Service;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

@Service
public class FileOperations {

    String fileName = "log.bin";

    BufferedOutputStream bufferedOutputStream;

    public FileOperations() {
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            this.bufferedOutputStream = new BufferedOutputStream(fos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToFile(byte[] data) {
        try {
            bufferedOutputStream.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeFile() {
        try {
            bufferedOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
