package com.example.bitcask.Recovery;

import lombok.Data;

@Data
public class Hint {

    private int stationId;
    private int fileIndex;
    private int offset;
    private Long timestamp;

    public Hint(int stationId, int fileIndex, int offset, Long timestamp) {
        this.stationId = stationId;
        this.fileIndex = fileIndex;
        this.offset = offset;
        this.timestamp = timestamp;
    }
}
