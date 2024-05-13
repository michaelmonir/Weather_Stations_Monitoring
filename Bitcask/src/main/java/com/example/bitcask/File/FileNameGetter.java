package com.example.bitcask.File;

public class FileNameGetter {

    public static String getFileName(int fileIndex) {
        return "Files/" + Integer.toString(fileIndex);
    }

    public static String getHintFileName(int fileIndex) {
        return getFileName(fileIndex) + ".hint";
    }

    public static String getNameWithPath(String fileName) {
        return "Files/" + fileName;
    }
}
