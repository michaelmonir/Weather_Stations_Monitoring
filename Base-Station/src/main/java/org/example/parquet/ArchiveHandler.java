package org.example.parquet;

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

public class ArchiveHandler {

    private final String archivePath;
    private final int batchSize;
    private final List<WeatherMessage> messageBuffer =new ArrayList<>();
    private final Map<String, ParquetFileWriter> parquetWriters = new HashMap<>();

    public ArchiveHandler(String archivePath, int batchSize) throws IOException {
        this.archivePath = archivePath;
        this.batchSize = batchSize;

        // Create archive directory if it doesn't exist
        FileSystem fs = FileSystem.get(new Configuration());
        Path archiveDirPath = new Path(archivePath);
        if (!fs.exists(archiveDirPath)) {
            fs.mkdirs(archiveDirPath);
        }
    }

    public void receiveWeatherMessage(WeatherMessage weatherMessage) throws IOException {
        messageBuffer.add(weatherMessage);
        if (messageBuffer.size() >= batchSize) {
            writeBufferToParquet(messageBuffer);
            messageBuffer.clear();
        }
    }

    private String getParquetFilePath(WeatherMessage weatherMessage) {
        String stationId = String.valueOf(weatherMessage.getStationId());
        String dateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date(weatherMessage.getStatusTimestamp()));
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
                try {
                    return new ParquetFileWriter(filePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            messageBuffer.computeIfAbsent(parquetFilePath, k -> new ArrayList<>()).add(message);
        }

        List<Thread> threads = new ArrayList<>();
//        System.out.println("messageBuffer: " + messageBuffer.size());
        for (Map.Entry<String, List<WeatherMessage>> entry : messageBuffer.entrySet()) {
            System.out.println("entry: " + entry.getKey() + " " + entry.getValue().size() );
            String parquetFilePath = entry.getKey();
            List<WeatherMessage> writeBuffer = entry.getValue();
//            ParquetFileWriter parquetWriter = parquetWriters.get(parquetFilePath);
//            try {
//                parquetWriter.write(writeBuffer);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            } finally {
//                writeBuffer.clear();
//                messageBuffer.remove(parquetFilePath);
////                    parquetWriter.close();
//            }
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
                e.printStackTrace();
            }
        }


    }
    public void close() throws IOException {
        for (ParquetFileWriter writer : parquetWriters.values()) {
            writer.close();
        }
    }

}

