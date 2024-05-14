package com.example.bitcask.NewMerge;

import com.example.bitcask.Exceptions.UnkownErrorException;

public class MergeScheduler implements Runnable {

    private static Thread thread;

    int numOfTimes = 0;
    int maxNumOfTimes;

    public MergeScheduler() {
        MergeScheduler.thread = new Thread(this);
        this.maxNumOfTimes = 1_000_000_000;
    }

    public MergeScheduler(int maxNumOfTimes) {
        MergeScheduler.thread = new Thread(this);
        this.maxNumOfTimes = maxNumOfTimes;
    }

    public void start() {
        MergeScheduler.thread.start();
    }

    public static void resume() {
        MergeScheduler.thread.interrupt();
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
            }
            Merger merger = new Merger();
            merger.run();
            this.numOfTimes++;
        }
    }
}
