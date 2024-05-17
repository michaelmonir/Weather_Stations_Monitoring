package com.example.bitcask.NewRecovery;

import com.example.bitcask.Exceptions.FinishedReadingFilesException;
import com.example.bitcask.File.ByteStreamFiles.ByteStreamReader;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Converters.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Segments.Segment;

public class SegmentRecoverer {

    MyMap myMap = new MyMap();
    private int segmentIndex;
    private ByteStreamReader streamReader;

    public SegmentRecoverer(int fileIndex) {
        this.segmentIndex = fileIndex;
        this.streamReader = new ByteStreamReader(Message.MESSAGE_SIZE, FileNameGetter.getFileName(fileIndex));
    }

    public Segment recover() {
        int offset = 0;
        while (true) {
            try {
                byte[] bytes = this.streamReader.read();
                Message message = new ByteToMessageConverter(bytes).convert();
                this.myMap.put(message.getStation_id(), message.getStatus_timestamp(), this.segmentIndex, offset);
                offset += Message.MESSAGE_SIZE;
            } catch (FinishedReadingFilesException e) {
                break;
            }
        }

        return new Segment(this.segmentIndex, this.myMap, offset);
    }
}
