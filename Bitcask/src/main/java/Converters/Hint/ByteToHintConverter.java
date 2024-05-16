package Converters.Hint;

import Converters.ByteToDatatypeConverters;
import com.example.bitcask.Recovery.Hint;

public class ByteToHintConverter {

    ByteToDatatypeConverters reader;

    public ByteToHintConverter(byte[] bytes) {
        this.reader = new ByteToDatatypeConverters(bytes);
    }

    public Hint convert() {
        int stationId = reader.readInt();
        int fileIndex = reader.readInt();
        int offset = reader.readInt();
        Long timestamp = reader.readLong();

        return new Hint(stationId, fileIndex, offset, timestamp);
    }
}
