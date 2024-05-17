package com.example.bitcask.NewRecovery;

import lombok.Data;

@Data
public class Hint {

    private Long stationId;
    private int fileIndex;
    private int offset;
    private Long timestamp;
    public static int HintSize = 24;

    public Hint(Long stationId, int fileIndex, int offset, Long timestamp) {
        this.stationId = stationId;
        this.fileIndex = fileIndex;
        this.offset = offset;
        this.timestamp = timestamp;
    }
}
