package com.example.bitcask.Converters.Hint;

import com.example.bitcask.Converters.ByteToDatatypeConverters;
import com.example.bitcask.NewRecovery.Hint;

public class ByteToHintConverter {

    ByteToDatatypeConverters reader;

    public ByteToHintConverter(byte[] bytes) {
        this.reader = new ByteToDatatypeConverters(bytes);
    }

    public Hint convert() {
        Long stationId = reader.readLong();
        int fileIndex = reader.readInt();
        int offset = reader.readInt();
        Long timestamp = reader.readLong();

        return new Hint(stationId, fileIndex, offset, timestamp);
    }
}
