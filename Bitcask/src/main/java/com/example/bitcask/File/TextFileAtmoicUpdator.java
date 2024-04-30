package com.example.bitcask.File;

import com.example.bitcask.Exceptions.FileException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TextFileAtmoicUpdator {

    private static String tempFileName = "tempFile.txt";

    public static void writeList(List<Integer> list, String fileName) {
        writeListToTemp(list);
        swapFilesAtomically(fileName);
    }

    private static void writeListToTemp(List<Integer> list) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFileName))) {
            for (Integer i : list) {
                writer.write(Integer.toString(i));
                writer.newLine();
            }
        } catch (IOException e) {
            throw new FileException();
        }
    }

    private static void swapFilesAtomically(String fileName) {
        try {
            java.nio.file.Files.move(java.nio.file.Paths.get(tempFileName),
                    java.nio.file.Paths.get(fileName),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.out.println("Failed to update file atomically.");
            throw new FileException();
        }
    }
}
