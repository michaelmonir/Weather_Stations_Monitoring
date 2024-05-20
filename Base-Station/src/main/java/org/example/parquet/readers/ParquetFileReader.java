package org.example.parquet.readers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

public class ParquetFileReader {


//    public static void main(String[] args) throws IOException {
//        String parquetFilePath = "/home/ahmed/Desktop/Weather_Stations_Monitoring/parquet-archive/0/1970-01-19_0.parquet";
//        Configuration conf = new Configuration();
//        Path path = new Path(parquetFilePath);
//        ParquetReader<Group> reader = ParquetReader.builder(
//                new GroupReadSupport()
//                , path).withConf(conf).build();
//
//        Group group = null;
//        try {
//            while ( (group = reader.read() )!= null) {
//                System.out.println(group);
//            }
//        } finally {
//            // Close the ParquetReader
//            if (reader != null) {
//                reader.close();
//            }
//        }
//    }
}
