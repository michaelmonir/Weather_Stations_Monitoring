package com.example.bitcask.Bitcask;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class BitcaskWriter {

    private HashMap<Long, Long> map;
    @Autowired
    private FileOperations fileOperations;

    public void write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();
        Long offset = fileOperations.writeToFileAndGetOffset(data);
        map.put(message.getStation_id(), offset);
    }

}
