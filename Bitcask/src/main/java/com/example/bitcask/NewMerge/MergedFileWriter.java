package com.example.bitcask.NewMerge;

import com.example.bitcask.File.BinaryFileOperations;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Hashmap.MyMap;
import com.example.bitcask.Segments.Segment;

import java.util.Iterator;
import java.util.Map;

public class MergedFileWriter {

    MyMap map;
    Segment resultSegment;

    public MergedFileWriter(MyMap map, Segment resultSegment) {
        this.map = map;
        this.resultSegment = resultSegment;
    }

    public void run() {
        this.writeMapToFile();
        this.writeHint();
    }

    private void writeMapToFile() {
        Iterator<Map.Entry<Long, MapEntry>> it = this.map.getIterator();
        while (it.hasNext()) {
            Map.Entry<Long, MapEntry> entry = it.next();

            MapEntry mapEntry = entry.getValue();
            String fileName = FileNameGetter.getFileName(mapEntry.fileIndex);

            byte[] bytes = BinaryFileOperations.readFromFile(fileName, mapEntry.offset);
            this.resultSegment.write(entry.getKey(), mapEntry.timestamp, bytes);
        }
    }

    private void writeHint() {
        // TODO
    }
}
