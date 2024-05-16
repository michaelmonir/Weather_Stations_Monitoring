package com.example.bitcask.NewRecovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.FileNameGetter;

import java.io.*;

public class RecoveryInformationUpdater {

    String recoveryFileName = FileNameGetter.getIndexesFileName();

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
