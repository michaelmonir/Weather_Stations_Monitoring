package com.example.bitcask.NewRecovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.File.TextFileOperations;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IndicesKeeper {

    private List<Integer> segmentIndices = new ArrayList<>();
    String recoveryFileName = FileNameGetter.getIndexesFileName();

    public List<Integer> recover() {
        String indexesFileName = FileNameGetter.getIndexesFileName();
        if (!TextFileOperations.fileExists(indexesFileName))
            return new ArrayList<>();
        String indexesFilePath = FileNameGetter.getIndexesFileName();
        this.getNumbersFromReader(indexesFilePath);
        return segmentIndices;
    }

    public void addSegment(int newSegmentIndex) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(recoveryFileName, true))) {
            writer.write(Integer.toString(newSegmentIndex));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new FileException();
        }
    }

    private void getNumbersFromReader(String filePath) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = reader.readLine()) != null) {
                int fileIndex = Integer.parseInt(line);
                segmentIndices.add(fileIndex);
            }
        } catch (IOException e) {
            throw new FileException();
        }
    }
}
