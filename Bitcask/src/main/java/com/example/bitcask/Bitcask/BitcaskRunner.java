package com.example.bitcask.Bitcask;

import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.MergeScheduler;

public class BitcaskRunner {

    public static void start(int maxSegmentSize, Long mergeScheduleTime) {
        Bitcask.setMaxSegmentSize(maxSegmentSize);
        Bitcask.setBitcask(new Bitcask());
        MergeScheduler.setInterval(mergeScheduleTime);
    }

    public static void put(long key) {
        Message message = new Message(key, key, (short)key, key, (int)key, (int)key, (int)key);
        Bitcask.getBitcask().write(message);
    }

    public static Message read(long key) {
        return Bitcask.getBitcask().read(key);
    }
}
