package com.example.bitcask.File.ByteStreamFiles;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.Exceptions.UnexpectedException;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;

public class ByteStreamWriter {

    private int chunkSize;
    private int blockSize;
    BufferedOutputStream outputStream;

    byte[] buffer;
    int offset = 0;

    public ByteStreamWriter(int blockSize, int numOfBlocks, String filePath) {


        this.blockSize = blockSize;
        this.chunkSize = numOfBlocks * blockSize;
        this.buffer = new byte[chunkSize];

        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(filePath));
        } catch(Exception e) {
            throw new FileException();
        }
    }

    public ByteStreamWriter(int blockSize, String filePath) {
        this(blockSize, 10_000, filePath);
    }

    public void write(byte[] bytes) {
        if (offset == this.blockSize) {
            writeBufferToDisk();
            offset = 0;
        }

        for (int i = 0; i < blockSize; i++)
            buffer[offset + i] = bytes[i];
        offset += blockSize;
    }

    private void writeBufferToDisk() {
        try {
            outputStream.write(this.buffer, 0, offset);
        } catch(Exception e) {
            throw new UnexpectedException();
        }
    }

    public void finishWriting() {
        try {
            this.writeBufferToDisk();
            this.outputStream.flush();
        } catch (Exception e) {
            throw new FileException();
        }
    }
}
