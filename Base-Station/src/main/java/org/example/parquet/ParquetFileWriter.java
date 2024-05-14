package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.*;
import org.example.models.WeatherMessage;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class ParquetFileWriter {
    private static final Schema parquetAvroSchema = new Schema.Parser().parse(
            "{\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"WeatherMessage\",\n" +
                    "  \"fields\": [\n" +
                    "    {\"name\": \"station_id\", \"type\": \"long\"},\n" +
                    "    {\"name\": \"s_no\", \"type\": \"long\"},\n" +
                    "    {\"name\": \"battery_status\", \"type\": \"string\"},\n" +
                    "    {\"name\": \"status_timestamp\", \"type\": \"long\"},\n" +
                    "    {\"name\": \"humidity\", \"type\": \"int\"},\n" +
                    "    {\"name\": \"temperature\", \"type\": \"int\"},\n" +
                    "    {\"name\": \"wind_speed\", \"type\": \"int\"}\n" +
                    "  ]\n" +
                    "}"
    );
    private final ParquetWriter<GenericRecord> writer;

    public ParquetFileWriter(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);

//        GenericRecord avroRecord = new GenericData.Record(parquetAvroSchema);
//        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path);
//        this.writer = builder.withConf(conf)
//                .withCompressionCodec(CompressionCodecName.SNAPPY)
//                .withType(parquetSchema)
//                .withPageSize(4 * 1024 * 1024)
//                .withDictionaryPageSize(1024 * 1024)
//                .withDictionaryEncoding(true)
//                .withValidation(false)
//                .build();
        this.writer=  AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(parquetAvroSchema)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();


//        FileSystem fs = FileSystem.get(conf);
//        fs.setWriteChecksum(false);
//        if (fs.exists(path)) {
//            // Open existing file for append
//            FSDataOutputStream out = fs.append(path);
//            writer = AvroParquetWriter.<GenericRecord>builder((OutputFile) out)
//                    .withSchema(parquetAvroSchema)
//                    .withConf(conf)
//                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .build();
//        } else {
//
//            HadoopOutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
//            writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
//                    .withSchema(parquetAvroSchema)
//                    .withConf(conf)
//                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .build();
//        }
    }

    public void write(List<WeatherMessage> buffer) throws IOException {
        for (WeatherMessage message : buffer) {
            writer.write(message.toAvroRecord(parquetAvroSchema));
        }
    }
    public void close() throws IOException {
        writer.close();
    }
}
