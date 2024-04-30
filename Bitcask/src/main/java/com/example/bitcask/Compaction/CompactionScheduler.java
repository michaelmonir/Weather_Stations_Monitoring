package com.example.bitcask.Compaction;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Compaction.IntervalMerging.IntervalMergerStrategySimpleImpl;
import com.example.bitcask.Exceptions.UnkownErrorException;

public class CompactionScheduler implements Runnable {

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new UnkownErrorException();
            }
            Compactor compactor = new Compactor(new CompactionStrategyImpl(), new IntervalMergerStrategySimpleImpl());
            compactor.compact();
            Bitcask bitcask = Bitcask.getBitcask();
            int z = 0;
            z++;
        }
    }
}
