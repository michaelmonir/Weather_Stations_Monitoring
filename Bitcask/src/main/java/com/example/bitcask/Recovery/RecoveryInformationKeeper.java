package com.example.bitcask.Recovery;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.TextFileOperations;
import com.example.bitcask.Segments.Segment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryInformationKeeper {

    private String fileName = "recoveryInformation.txt";

    public Bitcask recover() {
        if (!TextFileOperations.fileExists(fileName))
            return new Bitcask();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            int numOfFiles = Integer.parseInt(reader.readLine());
            List<Segment> segments = this.getSegments(numOfFiles, reader);

            return new Bitcask(segments);
        } catch (IOException e) {
            throw new FileException();
        }
    }

    private List<Segment> getSegments(int numOfFiles, BufferedReader reader) throws IOException {
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < numOfFiles; i++) {
            int fileIndex = Integer.parseInt(reader.readLine());
            SegmentRecovery segmentRecovery = new SegmentRecovery(fileIndex);
            Segment segment = segmentRecovery.recover();
            segments.add(segment);
        }
        if (numOfFiles == 0) segments.add(new Segment(1));
        return segments;
    }
}
