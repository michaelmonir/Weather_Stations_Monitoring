package com.example.bitcask.NewMerge;

import com.example.bitcask.Compaction.CompactionStrategy.CompactionStrategyImpl;
import com.example.bitcask.Compaction.Compactor;
import com.example.bitcask.Compaction.IntervalMerging.IntervalMergerStrategySimpleImpl;
import com.example.bitcask.Exceptions.UnkownErrorException;

public class MergeScheduler implements Runnable {

    private static Long interval = 1000L;

    public static void setInterval(Long interval) {
        MergeScheduler.interval = interval;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new UnkownErrorException();
            }
            Merger merger = new Merger();
            merger.run();
        }
    }
}
