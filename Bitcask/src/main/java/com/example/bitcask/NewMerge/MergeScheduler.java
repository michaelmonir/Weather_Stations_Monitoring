package com.example.bitcask.NewMerge;

import com.example.bitcask.Exceptions.UnkownErrorException;

public class MergeScheduler implements Runnable {

    int numOfTimes = 0;
    int maxNumOfTimes;


    public MergeScheduler() {
        this.maxNumOfTimes = 1_000_000_000;
    }

    public MergeScheduler(int maxNumOfTimes) {
        this.maxNumOfTimes = maxNumOfTimes;
    }

    private static Long interval = 1000L;

    public static void setInterval(Long interval) {
        MergeScheduler.interval = interval;
    }

    @Override
    public void run() {
        while (this.numOfTimes < this.maxNumOfTimes) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new UnkownErrorException();
            }
            Merger merger = new Merger();
            merger.run();
            this.numOfTimes++;
        }
    }
}
