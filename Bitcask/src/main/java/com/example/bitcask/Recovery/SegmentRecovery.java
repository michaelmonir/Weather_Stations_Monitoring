package com.example.bitcask.Recovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import Converters.Message.ByteToMessageConverter;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Segments.Segment;

import java.io.FileInputStream;
import java.util.HashMap;

public class SegmentRecovery {

    int size = 0;
    HashMap<Long, Integer> map;
    int fileIndex;
    FileInputStream fileInputStream;

    public SegmentRecovery(int fileIndex) {
        size = 0;
        map = new HashMap<>();
        this.fileIndex = fileIndex;
        fileInputStream = BinaryFileOperations.getFileInputStream(FileNameGetter.getFileName(fileIndex));
    }

    public Segment recover() {
        getHashMap();
        return null;
//        return new Segment(this.fileIndex, this.map, this.size);
    }

    private void getHashMap() {
        while (true) {
            byte[] data;
            try {
                data = BinaryFileOperations.readFromFile(fileInputStream);
            } catch (FileException e) {
                break;
            }
            long stationId = getStationIdFromByteArray(data);
            map.put(stationId, size);
            size += Message.MESSAGE_SIZE;
        }
    }

    private long getStationIdFromByteArray(byte[] data) {
        ByteToMessageConverter byteToMessageConverter = new ByteToMessageConverter(data);
        Message message = byteToMessageConverter.convert();
        return message.getStation_id();
    }
}
