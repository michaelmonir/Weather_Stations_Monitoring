package org.example.parquet.writers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.example.models.WeatherMessage;

import org.apache.spark.sql.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SparkArchiveHandler  implements Archiver{

    private final String archivePath;
    private final int batchSize;
    private final List<WeatherMessage> messageBuffer = new ArrayList<>();
    private final SparkSession spark;

    public SparkArchiveHandler(String archivePath, int batchSize) throws IOException {
        this.archivePath = archivePath;
        this.batchSize = batchSize;
        spark = SparkSession.builder()
                .appName("Spark Parquet Writer")
                .master("local[*]")
                .config("spark.executor.memory", "1g")
                .config("spark.driver.memory", "1g")
                .config("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
                .getOrCreate();
        FileSystem fs = FileSystem.get(new Configuration());
        Path archiveDirPath = new Path(archivePath);
        if (!fs.exists(archiveDirPath)) {
            fs.mkdirs(archiveDirPath);
        }

        //create the archive directory in if it does not exist


    }

    public void receiveWeatherMessage(WeatherMessage weatherMessage) {
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
        String parquetFileName = dateString + "_" + stationId;
        return stationDirPath + "/" + parquetFileName;
    }

    private void writeBufferToParquet(List<WeatherMessage> buffer) {
        Dataset<Row> data = getRowDataset(buffer);
        String outputPath = getParquetFilePath(buffer.get(0));
        data.write()
                .mode(SaveMode.Append)
                .option("compression", "snappy")
                .parquet(outputPath);
    }

    private Dataset<Row> getRowDataset(List<WeatherMessage> buffer) {
        List<Row> rows = new ArrayList<>();
        for (WeatherMessage weatherMessage : buffer) {
            rows.add(RowFactory.create(weatherMessage.getStationId(), weatherMessage.getSNo(), weatherMessage.getBatteryStatus(), weatherMessage.getStatusTimestamp(), weatherMessage.getWeather().getHumidity(), weatherMessage.getWeather().getTemperature(), weatherMessage.getWeather().getWindSpeed()));
        }
        StructType schema = new StructType()
                .add("stationId", DataTypes.IntegerType)
                .add("sNo", DataTypes.IntegerType)
                .add("batteryStatus", DataTypes.StringType)
                .add("statusTimestamp", DataTypes.LongType)
                .add("humidity", DataTypes.IntegerType)
                .add("temperature", DataTypes.IntegerType)
                .add("windSpeed", DataTypes.IntegerType);
        return spark.createDataFrame(rows, schema);
    }

    public void close() {
        if (spark != null) {
            spark.stop();
        }
    }
}
