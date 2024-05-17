package com.example.bitcask.Converters;

public class ByteToDatatypeConverters {

    byte[] bytes;
    int index;

    public ByteToDatatypeConverters(byte[] bytes) {
        this.bytes = bytes;
        this.index = 0;
    }

    public int readInt() {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (bytes[index++] & 0xFF);
        }
        return value;
    }

    public long readLong() {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[index++] & 0xFF);
        }
        return value;
    }

    public short readShort() {
        short value = 0;
        for (int i = 0; i < 2; i++) {
            value = (short)((value << 8) | (bytes[index++] & 0xFF));
        }
        return value;
    }
}
