package com.example.bitcask.NewRecovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.FileNameGetter;
import com.example.bitcask.File.TextFileOperations;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SegmentIndicesRecoverer {

    private List<Integer> segmentIndices = new ArrayList<>();

    public List<Integer> recover() {
        String indexesFileName = FileNameGetter.getIndexesFileName();
        if (!TextFileOperations.fileExists(indexesFileName))
            return new ArrayList<>();
        String indexesFilePath = FileNameGetter.getIndexesFileName();
        this.getNumbersFromReader(indexesFilePath);
        return segmentIndices;
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
