package com.example.bitcask.Recovery;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.FileException;
import com.example.bitcask.Segments.Segment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class RecoveryInformationKeeper {

    private String fileName = "recoveryInformation.txt";

    public Bitcask recover() {
        Bitcask bitcask = new Bitcask();

        if (!this.fileExists(fileName))
            return bitcask;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName);
            int numOfFiles = Integer.parseInt(reader.readLine());

            for (int i = 0; i < numOfFiles; i++) {
                int fileIndex = Integer.parseInt(reader.readLine());
                SegmentRecovery segmentRecovery = new SegmentRecovery(fileIndex);
                Segment segment = segmentRecovery.recover();
            }
        } catch (IOException e) {
            throw new FileException();
        }

        return bitcask;
    }

    private boolean fileExists(String fileName) {
        File file = new File(fileName);
        return file.exists();
    }
}
