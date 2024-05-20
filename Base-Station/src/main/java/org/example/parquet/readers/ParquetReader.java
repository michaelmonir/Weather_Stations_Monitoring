package org.example.parquet.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ParquetReader {
    private static final String ARCHIVE_PATH = "/home/ahmed/Desktop/Weather_Stations_Monitoring/parquet-archive";
    private final SparkSession spark;
    private final StructType schema = new StructType()
            .add("stationId", DataTypes.IntegerType)
            .add("sNo", DataTypes.IntegerType)
            .add("batteryStatus", DataTypes.StringType)
            .add("statusTimestamp", DataTypes.LongType)
            .add("humidity", DataTypes.IntegerType)
            .add("temperature", DataTypes.IntegerType)
            .add("windSpeed", DataTypes.IntegerType);

    public ParquetReader() {
        spark = SparkSession.builder()
                .appName("Spark Parquet Reader")
                .master("local[*]")
                .config("spark.executor.memory", "1g")
                .config("spark.driver.memory", "1g")
                .config("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
                .getOrCreate();
    }

    public List<Row> readAllParquetFiles(String directoryPath) {
        try {
            List<Path> parquetFiles = Files.walk(Path.of(directoryPath))
                    .filter(path -> path.toString().endsWith(".parquet"))
                    .collect(Collectors.toList());

            Dataset<Row> allParquetFiles = spark.read().schema(schema).parquet(parquetFiles.stream().map(Path::toString).toArray(String[]::new));
            allParquetFiles.show();
            return allParquetFiles.collectAsList();
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public List<Row> readAllParquetFiles() {
        Dataset<Row> parquetFile = spark.read().schema(schema).parquet(ARCHIVE_PATH + "/*" + "/*/" + "*.parquet");

        // Debugging: Show schema and count rows
        parquetFile.show();
        System.out.println("Parquet file schema:");
        parquetFile.printSchema();

        long rowCount = parquetFile.count();
        System.out.println("Number of rows: " + rowCount);
        return parquetFile.collectAsList();
    }

    //function to read all parquet files in the archive for a specific station
    public List<Row> readAllParquetFilesForStation(int stationId) {
        String stationPath = ARCHIVE_PATH + "/" + stationId;
        Dataset<Row> parquetFile = spark.read()
                .schema(schema)
                .parquet(stationPath);
        return parquetFile.collectAsList();
    }

    public void close() {
        spark.close();
    }

    public static void main(String[] args) {
        ParquetReader reader = new ParquetReader();
        List<Row> rows = reader.readAllParquetFiles(ARCHIVE_PATH);
        System.out.println("Reading all parquet files in the archive");
        System.out.println(rows.size() + " rows found");
        for (Row row : rows) {
            System.out.println(row);
        }
        reader.close();
    }
}
