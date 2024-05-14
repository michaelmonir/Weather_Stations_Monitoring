package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

public class ParquetFileReader {

    public static void main(String[] args) throws IOException {
        // Specify the path to the Parquet file to read
        String parquetFilePath = "/home/ahmed/Desktop/Weather_Stations_Monitoring/parquet-archive/0/1970-01-19_0.parquet";

        // Create a ParquetReader instance
        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        ParquetReader<Group> reader = ParquetReader.builder(
                new GroupReadSupport()
                , path).withConf(conf).build();

        Group group = null;
        try {
            // Read and print each row (or group) from the Parquet file
            while ( (group = reader.read() )!= null) {
                // Read the next row (or group) from the Parquet file

                System.out.println(group);
            }
//            while ((group = reader.read()) != null) {
//                System.out.println("Record:");
//                System.out.println("station_id: " + group.getInteger("station_id", 0));
//                System.out.println("s_no: " + group.getInteger("s_no", 0));
//                System.out.println("battery_status: " + group.getString("battery_status", 0));
//                System.out.println("status_timestamp: " + group.getLong("status_timestamp", 0));
//                System.out.println("humidity: " + group.getInteger("humidity", 0));
//                System.out.println("temperature: " + group.getInteger("temperature", 0));
//                System.out.println("wind_speed: " + group.getInteger("wind_speed", 0));
//                System.out.println("--------------------------------------");
//            }
        } finally {
            // Close the ParquetReader
            if (reader != null) {
                reader.close();
            }
        }
    }
}
