package com.example.bitcask.NewRecovery;

import com.example.bitcask.Converters.Hint.ByteToHintConverter;
import com.example.bitcask.Exceptions.FinishedReadingFilesException;
import com.example.bitcask.File.ByteStreamFiles.ByteStreamReader;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Segments.Segment;

public class SegmentWithHintRecoverer {

    MyMap myMap = new MyMap();
    private int segmentIndex;
    private ByteStreamReader streamReader;

    public SegmentWithHintRecoverer(int fileIndex) {
        this.segmentIndex = fileIndex;
        this.streamReader = new ByteStreamReader(Hint.HintSize, FileNameGetter.getHintFileName(fileIndex));
    }

    public Segment recover() {
        int offset = 0;
        while (true) {
            try {
                byte[] bytes = this.streamReader.read();
                Hint hint = new ByteToHintConverter(bytes).convert();
                this.myMap.put(hint.getStationId(), hint.getTimestamp(), this.segmentIndex, offset);
                offset += Message.MESSAGE_SIZE;
            } catch (FinishedReadingFilesException e) {
                break;
            }
        }

        return new Segment(this.segmentIndex, this.myMap, offset);
    }
}
