package com.example.bitcask.Bitcask;

import org.springframework.stereotype.Service;

@Service
public class BitcaskWriter {

    private String filename;

    private BitcaskWriter() {
        filename = "mic";
    }
}
