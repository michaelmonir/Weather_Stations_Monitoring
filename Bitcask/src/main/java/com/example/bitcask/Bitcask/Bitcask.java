package com.example.bitcask.Bitcask;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import com.example.bitcask.Segments.SegmentOperations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class Bitcask {

    private HashMap<Long, Integer> map;
    private SegmentOperations segmentOperations;

    public synchronized void write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();

        int offset = segmentOperations.writeToFileAndGetOffset(data);
        map.put(message.getStation_id(), offset);
    }

    public Message read(Long station_id) {
        int offset = map.get(station_id);
        byte[] data = segmentOperations.readFromFile(offset);
        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        return byteToMessageConverter.convert();
    }
}
