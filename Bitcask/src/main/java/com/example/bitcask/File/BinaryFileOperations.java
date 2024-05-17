package com.example.bitcask.File;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.Message.Message;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class BinaryFileOperations {

    public static void writeToFile(String fileName, byte[] data) {
        try {
            FileOutputStream fileOutputStream = getFileOutputStream(fileName, true);
            fileOutputStream.write(data);
        } catch (IOException e) {
            throw new FileException();
        }
    }

    public static byte[] readFromFile(String fileName, int offset) {
        try {
            FileInputStream fileInputStream = getFileInputStream(fileName);
            byte[] data = new byte[Message.MESSAGE_SIZE];
            fileInputStream.skip(offset);
            fileInputStream.read(data, 0, Message.MESSAGE_SIZE);
            return data;
        } catch (IOException e) {
            throw new FileException();
        }
    }

    public static byte[] readFromFile(FileInputStream fileInputStream) {
        try {
            byte[] data = new byte[Message.MESSAGE_SIZE];
            int numOfReadBytes = fileInputStream.read(data, 0, Message.MESSAGE_SIZE);
            handleInsufficientBytesRead(numOfReadBytes);
            return data;
        } catch (IOException e) {
            throw new FileException();
        }
    }

    public static FileInputStream getFileInputStream(String fileName) {
        try {
            return new FileInputStream(fileName);
        } catch (IOException e) {
            throw new FileException();
        }
    }

    public static FileOutputStream getFileOutputStream(String fileName, boolean append) {
        try {
            return new FileOutputStream(fileName, append);
        } catch (IOException e) {
            e.printStackTrace();
            throw new FileException();
        }
    }

    private static void handleInsufficientBytesRead(int numOfReadBytes) {
        if (numOfReadBytes == -1) {
            throw new FileException();
        }
    }
}
