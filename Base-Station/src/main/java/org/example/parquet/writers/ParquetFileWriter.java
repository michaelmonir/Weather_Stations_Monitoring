package org.example.parquet.writers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.example.models.WeatherMessage;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
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
        List<Group> groups = null;
        if (path.getFileSystem(conf).exists(path)) {
            try (ParquetReader<Group> reader = ParquetReader.builder(
                    new GroupReadSupport()
                    , path).withConf(conf).build()){
                groups = new ArrayList<>();
                Group group;
                while ((group = reader.read()) != null) {
                    groups.add(group);
                }
            }
        }
        path.getFileSystem(conf).delete(path, false);
        OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
        this.writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(parquetAvroSchema)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
        if (groups != null) {
            for (Group group : groups) {
                GenericRecord record = convertToAvroRecord(group);
                writer.write( record);
            }
        }
    }

    private GenericRecord convertToAvroRecord(Group group) {
        GenericRecord record = new org.apache.avro.generic.GenericData.Record(parquetAvroSchema);
        record.put("station_id", group.getLong("station_id", 0));
        record.put("s_no", group.getLong("s_no", 0));
        record.put("battery_status", group.getString("battery_status", 0));
        record.put("status_timestamp", group.getLong("status_timestamp", 0));
        record.put("humidity", group.getInteger("humidity", 0));
        record.put("temperature", group.getInteger("temperature", 0));
        record.put("wind_speed", group.getInteger("wind_speed", 0));
        return record;
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
