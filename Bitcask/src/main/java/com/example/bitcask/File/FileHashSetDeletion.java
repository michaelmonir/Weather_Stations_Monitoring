package com.example.bitcask.File;

import java.io.File;
import java.util.HashSet;

public class FileHashSetDeletion {

    public static void deleteFiles(HashSet<String> fileSet) {
        File folder = new File("Files/");
        File[] files = folder.listFiles();

        for (File file : files) {
            String fileName = FileNameGetter.getNameWithPath(file.getName());

            if (!fileSet.contains(fileName))
                file.delete();
        }
    }
}
