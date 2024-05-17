package com.example.bitcask.NewRecovery;

import com.example.bitcask.Converters.Hint.HintToByteConverter;
import com.example.bitcask.File.ByteStreamFiles.ByteStreamWriter;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Hashmap.MapEntry;
import com.example.bitcask.Segments.Segment;

import java.util.Iterator;
import java.util.Map;

public class HintWriter {

    private Segment segment;
    private ByteStreamWriter byteStreamWriter;

    public HintWriter(Segment segment) {
        this.segment = segment;
        String hintFileName = FileNameGetter.getHintFileName(segment.getIndex());
        this.byteStreamWriter = new ByteStreamWriter(Hint.HintSize, hintFileName);
    }

    public void run() {
        Iterator iterator = this.segment.getMyMap().getIterator();

        while (iterator.hasNext()) {
            Map.Entry<Long, MapEntry> entry = (Map.Entry<Long, MapEntry>) iterator.next();
            Hint hint = new Hint(entry.getKey(),
                    entry.getValue().fileIndex,
                    entry.getValue().offset,
                    entry.getValue().timestamp);
            byte[] bytes = new HintToByteConverter(hint).convert();
            byteStreamWriter.write(bytes);
        }
        byteStreamWriter.finishWriting();
    }
}
