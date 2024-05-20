package com.example.bitcask.Bitcask;

import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewRecovery.Recoverer;

public class BitcaskRunner {

    public static void start(int maxSegmentSize, Long mergeScheduleTime, Bitcask bitcask) {
        Bitcask.setMaxSegmentSize(maxSegmentSize);
        Bitcask.setBitcask(bitcask);
        MergeScheduler.setInterval(mergeScheduleTime);

        MergeScheduler mergeScheduler = new MergeScheduler();
        mergeScheduler.start();
    }

    public static void start(int maxSegmentSize, Long mergeScheduleTime) {
        start(maxSegmentSize, mergeScheduleTime, new Bitcask());
    }

    public static void start(int maxSegmentSize, int maxNumOfSegments, long mergeScheduleTime) {
        Bitcask.setMaxNumOfSegments(maxNumOfSegments);
        start(maxSegmentSize, mergeScheduleTime);
    }

    public static void startAndRecover(int maxSegmentSize, Long mergeScheduleTime) {
        Bitcask recovered = new Recoverer().recover();
        start(maxSegmentSize, mergeScheduleTime, recovered);
    }

    public static void put(long key) {
        Message message = new Message(key);
        Bitcask.getBitcask().write(message);
    }

    public static Message read(long key) {
        return Bitcask.getBitcask().read(key);
    }
}
