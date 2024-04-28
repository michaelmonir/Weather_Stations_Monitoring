package com.example.bitcask.Message;

import org.springframework.stereotype.Service;

public class ByteToMessageConverter {

    byte[] bytes;
    int index;

    public ByteToMessageConverter(byte[] bytes) {
        this.bytes = bytes;
        this.index = 0;
    }

    public Message convert() {
        Long station_id = readLong();
        Long s_no = readLong();
        short battery_status = readShort();
        Long status_timestamp = readLong();
        int humidity = readInt();
        int temperature = readInt();
        int wind_speed = readInt();
        return new Message(station_id, s_no, battery_status, status_timestamp, humidity, temperature, wind_speed);
    }

    private int readInt() {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (bytes[index++] & 0xFF);
        }
        return value;
    }

    private long readLong() {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[index++] & 0xFF);
        }
        return value;
    }

    private long readShort() {
        short value = 0;
        for (int i = 0; i < 2; i++) {
            value = (short)((value << 8) | (bytes[index++] & 0xFF));
        }
        return value;
    }
}
