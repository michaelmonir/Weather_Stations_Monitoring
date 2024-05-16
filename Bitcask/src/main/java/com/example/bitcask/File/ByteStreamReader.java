package com.example.bitcask.File;

import com.example.bitcask.Exceptions.FinishedReadingFilesException;
import com.example.bitcask.Exceptions.UnexpectedException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;

public class ByteStreamReader {

    private int chunkSize;
    private int blockSize;
    BufferedInputStream inputStream;
    long currentBlockSize = 0;

    byte[] bytes;
    int offset = 0;

    public ByteStreamReader(int blockSize, int numOfBlocks, String filePath) {
        this.blockSize = blockSize;
        this.chunkSize = numOfBlocks * blockSize;
        this.bytes = new byte[chunkSize];
        try {
            inputStream = new BufferedInputStream(new FileInputStream(filePath));
        } catch(Exception e) {
        }
    }

    public ByteStreamReader(int blockSize, String filePath) {
        this(blockSize, 10_000, filePath);
    }

    public byte[] read() {
        if (offset == currentBlockSize) {
            offset = 0;
            readBufferFromFile();
        }

        byte[] ans = new byte[blockSize];
        for (int i = 0; i < blockSize; i++)
            ans[i] = bytes[offset + i];
        offset += blockSize;

        return ans;
    }

    private void readBufferFromFile() {
        try {
            this.currentBlockSize = inputStream.read(this.bytes);
        } catch(Exception e) {
            throw new UnexpectedException();
        }
        if (this.currentBlockSize == -1)
            throw new FinishedReadingFilesException();
    }
}
