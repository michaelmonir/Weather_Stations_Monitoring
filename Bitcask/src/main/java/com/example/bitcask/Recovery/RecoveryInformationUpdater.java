package com.example.bitcask.Recovery;

import com.example.bitcask.Exceptions.FileException;
import com.example.bitcask.File.TextFileOperations;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RecoveryInformationUpdater {

    String recoveryFileName = "recoveryInformation.txt";

    public void addSegment(int segmentIndex) {
        List<Integer> list = this.getIntegerListFromRecoveryFile();
        list.add(segmentIndex);
        updateRecoveryFile(list);
    }

    private List<Integer> getIntegerListFromRecoveryFile() {
        if (!TextFileOperations.fileExists(recoveryFileName))
            return new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(recoveryFileName));
            String ss = reader.readLine();
            int numOfFiles = Integer.parseInt(ss);

            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < numOfFiles; i++)
                list.add(Integer.parseInt(reader.readLine()));
            return list;
        } catch (IOException e) {
            throw new FileException();
        }
    }

    public void updateRecoveryFile(List<Integer> list) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(recoveryFileName))) {
            writer.write(Integer.toString(list.size()));
            writer.newLine();

            for (int segmentIndex : list) {
                writer.write(Integer.toString(segmentIndex));
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            throw new FileException();
        }
    }
}
