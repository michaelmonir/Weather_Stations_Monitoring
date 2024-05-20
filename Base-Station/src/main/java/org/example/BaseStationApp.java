package org.example;

import org.example.apis.InvalidMessageChannel;
import org.example.apis.KafkaMessageConsumer;
import org.example.models.WeatherMessage;
import org.example.parquet.writers.Archiver;
import org.example.parquet.writers.SparkArchiveHandler;
import org.example.validators.MessageValidator;
import com.networknt.schema.ValidationMessage;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseStationApp {

    private static final String SCHEMA_FILE_PATH = "/home/ahmed/Desktop/Weather_Stations_Monitoring/Base-Station/src/main/resources/weather-station-schema.json";

    private static final String TOPIC_NAME = "weather-station";

    private static final String INVALID_MESSAGE_TOPIC = "invalid-messages";
    private static final String ARCHIVE_TOPIC = "parquet-archive";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final AtomicBoolean running = new AtomicBoolean(true);

//        {"station_id": 123, "s_no": 1, "battery_status": "high", "status_timestamp": 1633494000, "weather": {"humidity": 50, "temperature": 25, "wind_speed": 15}}
//    public static void main(String[] args) throws Exception {


//        Archiver archiveHandler = new SparkArchiveHandler(ARCHIVE_TOPIC, 60);
//        int stationId;
//        for (int i = 0; i < 120; i++) {
//            if (i<60) {
//                stationId = 0;
//            } else {
//                stationId = 1;
//            }
//            String message = "{\"station_id\": " + stationId + ", \"s_no\": 1, \"battery_status\": \"high\", \"status_timestamp\": 1633494000, \"weather\": {\"humidity\": " + i + ", \"temperature\": " + (i + 1000) + ", \"wind_speed\": " + (i - 200) + "}}";
//            WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
//            archiveHandler.receiveWeatherMessage(weatherMessage);
//        }
//        archiveHandler.close();
//
//
//    }
public static void main(String[] args) {
    try {
        InputStream schemaStream = Files.newInputStream(Paths.get(SCHEMA_FILE_PATH));
        KafkaMessageConsumer kafkaConsumer = new KafkaMessageConsumer(TOPIC_NAME, BOOTSTRAP_SERVERS);
        MessageValidator validator = new MessageValidator(schemaStream);
        InvalidMessageChannel invalidMessageChannel = new InvalidMessageChannel(INVALID_MESSAGE_TOPIC, BOOTSTRAP_SERVERS);
        Archiver archiveHandler = new SparkArchiveHandler(ARCHIVE_TOPIC, 3);




        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running.set(false);
            kafkaConsumer.close();
            try {
                archiveHandler.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        while (running.get()) {
            List<String> messages = kafkaConsumer.consumeMessages();
            for (String message : messages) {
                Set<ValidationMessage> errors = validator.validateAndProcessMessage(message);
                if (errors != null && !errors.isEmpty()) {
                    invalidMessageChannel.publishInvalidMessage(message, errors);
                    continue;
                }
                WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
                archiveHandler.receiveWeatherMessage(weatherMessage);

            }
        }
    } catch (Exception e) {
        System.out.println("Error: " + e.getMessage());
    }
}
}
