package com.example.bitcask.Recovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.TextFileOperations;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RecoveryInformationUpdater {

    String recoveryFileName = "recoveryInformation.txt";

    public void addSegment(int newSegmentIndex) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(recoveryFileName, true))) {
            writer.write(Integer.toString(newSegmentIndex));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new FileException();
        }
    }
}
