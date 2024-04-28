package com.example.bitcask.File;

import com.example.bitcask.Message.Message;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileOperations {

    public static int writeToFile(FileOutputStream fileOutputStream, byte[] data) {
        try {
            fileOutputStream.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFromFile(FileInputStream fileInputStream, int offset) {
        try {
            byte[] data = new byte[Message.MESSAGE_SIZE];
            fileInputStream.read(data, offset, Message.MESSAGE_SIZE);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void closeFile(FileOutputStream fileOutputStream, FileInputStream fileInputStream) {
        try {
            fileOutputStream.close();
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
