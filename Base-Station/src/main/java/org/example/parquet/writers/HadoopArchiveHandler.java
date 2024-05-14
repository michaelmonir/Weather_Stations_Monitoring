package org.example.parquet.writers;

import org.example.models.WeatherMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.util.*;

public class HadoopArchiveHandler implements Archiver {

    private final String archivePath;
    private final int batchSize;
    private final List<WeatherMessage> messageBuffer =new ArrayList<>();
    private final Map<String, ParquetFileWriter> parquetWriters = new HashMap<>();

    private final Map<Integer,List<String>> fileMap = new HashMap<>();

    public HadoopArchiveHandler(String archivePath, int batchSize) throws IOException {
        this.archivePath = archivePath;
        this.batchSize = batchSize;
        FileSystem fs = FileSystem.get(new Configuration());
        Path archiveDirPath = new Path(archivePath);
        if (!fs.exists(archiveDirPath)) {
            fs.mkdirs(archiveDirPath);
        }

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                for (Map.Entry<Integer, List<String>> entry : fileMap.entrySet()) {
                    List<String> files = entry.getValue();
                    if (files.size() > 2) {
                        String oldestFile = files.remove(0);
                        ParquetFileWriter writer = parquetWriters.remove(oldestFile);
                        try {
                            writer.close();
                        } catch (IOException e) {
                            System.out.println("Error closing writer: " + e.getMessage());
                        }
                    }
                }
            }
        }, 0, 5000);


    }

    @Override

    public void receiveWeatherMessage(WeatherMessage weatherMessage) {
        messageBuffer.add(weatherMessage);
        if (messageBuffer.size() >= batchSize) {
            writeBufferToParquet(messageBuffer);
            messageBuffer.clear();
        }
    }

    private String getParquetFilePath(WeatherMessage weatherMessage) {
        String stationId = String.valueOf(weatherMessage.getStationId());
        //string data format for the file name have to be in the format of "yyyy-MM-dd-HH-mm-ss"
        String dateString = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date(weatherMessage.getStatusTimestamp()));

//        String dateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date(weatherMessage.getStatusTimestamp()));
        String stationDirPath = archivePath + "/" + stationId;
        String PARQUET_EXTENSION = ".parquet";
        String parquetFileName = dateString + "_" + stationId + PARQUET_EXTENSION;
        return stationDirPath + "/" + parquetFileName;
    }



    private void writeBufferToParquet(List<WeatherMessage> buffer){

        Map <String, List<WeatherMessage>> messageBuffer = new HashMap<>();

        for (WeatherMessage message : buffer) {
            String parquetFilePath = getParquetFilePath(message);

            parquetWriters.computeIfAbsent(parquetFilePath, filePath -> {
                fileMap.computeIfAbsent(message.getStationId(), k -> new ArrayList<>()).add(parquetFilePath);
                try {
                    return new ParquetFileWriter(filePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            messageBuffer.computeIfAbsent(parquetFilePath, k -> new ArrayList<>()).add(message);
        }

        List<Thread> threads = new ArrayList<>();
        for (Map.Entry<String, List<WeatherMessage>> entry : messageBuffer.entrySet()) {
            System.out.println("entry: " + entry.getKey() + " " + entry.getValue().size() );
            String parquetFilePath = entry.getKey();
            List<WeatherMessage> writeBuffer = entry.getValue();
            threads.add(new Thread(() -> {
                ParquetFileWriter parquetWriter = parquetWriters.get(parquetFilePath);
                try {
                    parquetWriter.write(writeBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    writeBuffer.clear();
                    messageBuffer.remove(parquetFilePath);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted: " + e.getMessage());
            }
        }


    }
    public void close() throws IOException {
        for (ParquetFileWriter writer : parquetWriters.values()) {
            writer.close();
        }
    }

}

