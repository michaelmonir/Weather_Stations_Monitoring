package com.example.bitcask.Converters;

import lombok.Getter;

public class DatatypeToByteConverters {

    int index;
    @Getter
    byte[] bytes;

    public DatatypeToByteConverters(int numOfBytes) {
        this.bytes = new byte[numOfBytes];
        this.index = 0;
    }

    public void writeInt(int value) {
        for (int i = 0; i < 4; i++)
            bytes[index++] = (byte)(value >> (8 * (3 - i)));
    }

    public void writeLong(long value) {
        for (int i = 0; i < 8; i++)
            bytes[index++] = (byte)(value >> (8 * (7 - i)));
    }

    public void writeShort(short value) {
        for (int i = 0; i < 2; i++)
            bytes[index++] = (byte)(value >> (8 * (1 - i)));
    }
}
