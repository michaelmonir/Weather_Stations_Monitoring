package com.example.bitcask.Bitcask;

import com.example.bitcask.File.FileOperations;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Message.MessageToByteConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BitcaskWriter {

    @Autowired
    private FileOperations fileOperations;

    public void write(Message message) {
        MessageToByteConverter messageToByteConverter = new MessageToByteConverter(message);
        byte[] data = messageToByteConverter.convert();
        fileOperations.writeToFile(data);
    }
}
