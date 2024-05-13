package com.example.bitcask.Recovery;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.SegmentIncreamenter;
import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.TextFileOperations;
import com.example.bitcask.Segments.Segment;

import java.io.BufferedReader;
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
            List<Segment> segments = this.getSegments(reader);

            SegmentIncreamenter.recover(segments);
//            return new Bitcask(segments);
            return new Bitcask();
        } catch (IOException e) {
            throw new FileException();
        }
    }

    private List<Segment> getSegments(BufferedReader reader) throws IOException {
        List<Segment> segments = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            int fileIndex = Integer.parseInt(line);
            SegmentRecovery segmentRecovery = new SegmentRecovery(fileIndex);
            Segment segment = segmentRecovery.recover();
            segments.add(segment);
        }
        if (segments.isEmpty()) {
            segments.add(new Segment(1));
        }
        return segments;
    }
}
