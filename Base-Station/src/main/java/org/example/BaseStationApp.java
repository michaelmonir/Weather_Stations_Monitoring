package org.example;

import com.networknt.schema.ValidationMessage;
import org.example.apis.InvalidMessageChannel;
import org.example.apis.KafkaMessageConsumer;
import org.example.models.WeatherMessage;
import org.example.parquet.ArchiveHandler;
import org.example.validators.MessageValidator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseStationApp {

    private static final String SCHEMA_FILE_PATH = "weather-station-schema.json";
    private static final String TOPIC_NAME = "weather-station";
    private static final String INVALID_MESSAGE_TOPIC = "invalid-messages";
    private static final String ARCHIVE_TOPIC = "parquet-archive";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final AtomicBoolean running = new AtomicBoolean(true);


    public static void main(String[] args) throws IOException {
//        {"station_id": 123, "s_no": 1, "battery_status": "high", "status_timestamp": 1633494000, "weather": {"humidity": 50, "temperature": 25, "wind_speed": 15}}

            ArchiveHandler archiveHandler = new ArchiveHandler(ARCHIVE_TOPIC, 3);
//            archiveHandler.receiveWeatherMessage(weatherMessage);
            for (int i = 0; i < 120; i++) {
            String message = "{\"station_id\": "+0+", \"s_no\": 1, \"battery_status\": \"high\", \"status_timestamp\": 1633494000, \"weather\": {\"humidity\": "+i+", \"temperature\": "+(i+20)+", \"wind_speed\": "+(i+30)+"}}";
            WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
                archiveHandler.receiveWeatherMessage(weatherMessage);
            }
            archiveHandler.close();


    }
//    public static void main(String[] args) {
//        try {
//            InputStream schemaStream = BaseStationApp.class.getClassLoader().getResourceAsStream(SCHEMA_FILE_PATH);
//            KafkaMessageConsumer kafkaConsumer = new KafkaMessageConsumer(TOPIC_NAME, BOOTSTRAP_SERVERS);
//            MessageValidator validator = new MessageValidator(schemaStream);
//            InvalidMessageChannel invalidMessageChannel = new InvalidMessageChannel(INVALID_MESSAGE_TOPIC, BOOTSTRAP_SERVERS);
//            ArchiveHandler archiveHandler = new ArchiveHandler(ARCHIVE_TOPIC, 3);
//
//
//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                System.out.println("Shutting down...");
//                running.set(false);
//                kafkaConsumer.close();
//            }));
//            while (running.get()) {
//                List<String> messages = kafkaConsumer.consumeMessages();
//                for (String message : messages) {
//                    Set<ValidationMessage> errors = validator.validateAndProcessMessage(message);
//                    if (errors != null && !errors.isEmpty()) {
//                        invalidMessageChannel.publishInvalidMessage(message, errors);
//                        continue;
//                    }
//                    WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
//                    System.out.println(weatherMessage);
//                    archiveHandler.receiveWeatherMessage(weatherMessage);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
