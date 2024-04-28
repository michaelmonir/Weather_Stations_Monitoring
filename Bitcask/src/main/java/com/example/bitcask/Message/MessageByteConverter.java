package com.example.bitcask.Message;

import org.springframework.stereotype.Service;

@Service
public class MessageByteConverter {

    public byte[] convertToBytes(Message message) {
        byte[] bytes = new byte[32];
        return bytes;
    }

    public Message convertToMessage(byte[] bytes) {
        return new Message(1L, 1L, (short)0, 1L, 1, 1, 1);
    }
}
