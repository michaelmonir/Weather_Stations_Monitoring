package com.example.bitcask.Message;

public class MessageToByteConverter {

    Message message;
    int index;
    byte[] bytes;

    public MessageToByteConverter(Message message) {
        this.message = message;
        this.index = 0;
        this.bytes = new byte[Message.MESSAGE_SIZE];
    }

    public byte[] convert() {
        writeLong(message.getStation_id());
        writeLong(message.getS_no());
        writeShort(message.getBattery_status());
        writeLong(message.getStatus_timestamp());
        writeInt(message.getHumidity());
        writeInt(message.getTemperature());
        writeInt(message.getWind_speed());
        return this.bytes;
    }

    private void writeInt(int value) {
        for (int i = 0; i < 4; i++) {
            bytes[index++] = (byte)(value >> (8 * (3 - i)));
        }
    }

    private void writeLong(long value) {
        for (int i = 0; i < 8; i++) {
            bytes[index++] = (byte)(value >> (8 * (7 - i)));
        }
    }

    private void writeShort(short value) {
        for (int i = 0; i < 2; i++) {
            bytes[index++] = (byte)(value >> (8 * (1 - i)));
        }
    }
}
