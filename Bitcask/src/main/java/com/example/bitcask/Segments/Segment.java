package com.example.bitcask.Segments;

import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Converters.Message.MessageToByteConverter;
import lombok.Getter;

public class Segment {

    @Getter
    private int index;
    @Getter
    private MyMap myMap;
    @Getter
    private int size;

    public Segment(int index) {
        this.size = 0;
        this.index = index;
        this.myMap = new MyMap();
    }

    public Segment(int index, MyMap myMap, int size) {
        this.index = index;
        this.myMap = myMap;
        this.size = size;
    }

    public int writeToFileAndGetOffset(byte[] data) {
        BinaryFileOperations.writeToFile(FileNameGetter.getFileName(this.index), data);

        int ret = size;
        size += Message.MESSAGE_SIZE;
        return ret;
    }

    public MapEntry write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] bytes = messageToByteConverter.convert();

        int offset = this.writeToFileAndGetOffset(bytes);

        return myMap.put(message.getStation_id(), message.getStatus_timestamp(), this.index, offset);
    }

    public MapEntry write(Long stationId, Long timeStamp, byte[] bytes) {
        int offset = this.writeToFileAndGetOffset(bytes);

        return myMap.put(stationId, timeStamp, this.index, offset);
    }
}
