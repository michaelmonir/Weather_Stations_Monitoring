package com.example.bitcask.Segments;

import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import lombok.Getter;

public class Segment {

    @Getter
    private int segmentIndex;
    @Getter
    private MyMap myMap;
    @Getter
    private int size;

    public Segment(int segmentIndex) {
        this.size = 0;
        this.segmentIndex = segmentIndex;
        this.myMap = new MyMap();
    }

    public Segment(int segmentIndex, MyMap myMap, int size) {
        this.segmentIndex = segmentIndex;
        this.myMap = myMap;
        this.size = size;
    }

    public int writeToFileAndGetOffset(byte[] data) {
        BinaryFileOperations.writeToFile(FileNameGetter.getFileName(this.segmentIndex), data);

        int ret = size;
        size += Message.MESSAGE_SIZE;
        return ret;
    }

    public boolean hasStation(long stationId) {
        return this.myMap.containsKey(stationId);
    }

    public MapEntry write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] bytes = messageToByteConverter.convert();

        int offset = this.writeToFileAndGetOffset(bytes);

        return myMap.put(message.getStation_id(), message.getStatus_timestamp(), this.segmentIndex, offset);
    }

    public Message read(Long stationId) {
        int offset = myMap.get(stationId).offset;
        byte[] data = BinaryFileOperations
                .readFromFile(FileNameGetter.getFileName(this.segmentIndex), offset);

        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }
}
