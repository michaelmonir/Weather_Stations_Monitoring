package com.example.bitcask.NewMerge;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.File.FileHashSetDeletion;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.Segments.Segment;

import java.util.HashSet;

public class FileCleaner {

    public static void cleanExtraFiles(Bitcask bitcask) {
        HashSet<String> files = new HashSet<>();
        for (Segment s : bitcask.getSegments()) {
            files.add(FileNameGetter.getFileName(s.getIndex()));
            files.add(FileNameGetter.getHintFileName(s.getIndex()));
        }
        FileHashSetDeletion.deleteFiles(files);
    }
}