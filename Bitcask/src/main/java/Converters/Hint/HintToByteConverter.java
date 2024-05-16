package Converters.Hint;

import Converters.DatatypeToByteConverters;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Recovery.Hint;

public class HintToByteConverter {

    private Hint hint;
    private DatatypeToByteConverters writer;

    public HintToByteConverter(Hint hint) {
        this.hint = hint;
        this.writer = new DatatypeToByteConverters(Message.MESSAGE_SIZE);
    }

    public byte[] convert() {
        writer.writeLong(hint.getStationId());
        writer.writeLong(hint.getFileIndex());
        writer.writeInt(hint.getOffset());
        writer.writeLong(hint.getTimestamp());

        return this.writer.getBytes();
    }
}
