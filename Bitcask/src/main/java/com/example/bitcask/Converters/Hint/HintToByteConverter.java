package com.example.bitcask.Converters.Hint;

import com.example.bitcask.Converters.DatatypeToByteConverters;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewRecovery.Hint;

public class HintToByteConverter {

    private Hint hint;
    private DatatypeToByteConverters writer;

    public HintToByteConverter(Hint hint) {
        this.hint = hint;
        this.writer = new DatatypeToByteConverters(Hint.HintSize);
    }

    public byte[] convert() {
        writer.writeLong(hint.getStationId());
        writer.writeInt(hint.getFileIndex());
        writer.writeInt(hint.getOffset());
        writer.writeLong(hint.getTimestamp());

        return this.writer.getBytes();
    }
}
