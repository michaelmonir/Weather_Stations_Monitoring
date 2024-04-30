package com.example.bitcask.File;

import java.io.File;

public class TextFileOperations {

    public static boolean fileExists(String fileName) {
        return new File(fileName).exists();
    }
}
