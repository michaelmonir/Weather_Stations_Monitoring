package org.example.parquet.writers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.example.models.WeatherMessage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SparkArchiveHandler implements Archiver {

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
    }

    public void receiveWeatherMessage(WeatherMessage weatherMessage) {
        messageBuffer.add(weatherMessage);
        if (messageBuffer.size() >= batchSize) {
            writeBufferToParquet(messageBuffer);
            messageBuffer.clear();
        }
    }
    private void writeBufferToParquet(List<WeatherMessage> buffer) {
        Dataset<Row> data = getRowDataset(buffer);
        data = data.withColumn("date", functions.from_unixtime(data.col("statusTimestamp").divide(1000), "yyyy-MM-dd"))
                .withColumn("Id", functions.col("stationId").cast(DataTypes.IntegerType));
        data.write()
                .mode(SaveMode.Append)
                .option("mergeSchema", "true")
                .partitionBy("date", "Id")
                .option("compression", "snappy")
                .parquet(archivePath);
    }

    private Dataset<Row> getRowDataset(List<WeatherMessage> buffer) {
        List<Row> rows = new ArrayList<>();
        for (WeatherMessage weatherMessage : buffer) {
            rows.add(RowFactory.create(
                    weatherMessage.getStationId(),
                    weatherMessage.getSNo(),
                    weatherMessage.getBatteryStatus(),
                    weatherMessage.getStatusTimestamp(),
                    weatherMessage.getWeather().getHumidity(),
                    weatherMessage.getWeather().getTemperature(),
                    weatherMessage.getWeather().getWindSpeed()
            ));
        }
        StructType schema = new StructType()
                .add("stationId", DataTypes.IntegerType,false)
                .add("sNo", DataTypes.IntegerType,false)
                .add("batteryStatus", DataTypes.StringType,false)
                .add("statusTimestamp", DataTypes.LongType,false)
                .add("humidity", DataTypes.IntegerType,false)
                .add("temperature", DataTypes.IntegerType,false)
                .add("windSpeed", DataTypes.IntegerType,false);

        return spark.createDataFrame(rows, schema);
    }

    public void close() {
        if (spark != null) {
            spark.stop();
        }
    }
}

