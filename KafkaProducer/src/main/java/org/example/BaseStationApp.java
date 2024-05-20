package org.example;

import com.networknt.schema.ValidationMessage;
import org.example.apis.KafkaMessageConsumer;
import org.example.models.WeatherMessage;
import org.example.validators.MessageValidator;

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

    public static void main(String[] args) {
        try {
            InputStream schemaStream = BaseStationApp.class.getClassLoader().getResourceAsStream(SCHEMA_FILE_PATH);
            KafkaMessageConsumer kafkaConsumer = new KafkaMessageConsumer(TOPIC_NAME, BOOTSTRAP_SERVERS);
            MessageValidator validator = new MessageValidator(schemaStream);
//            InvalidMessageChannel invalidMessageChannel = new InvalidMessageChannel(INVALID_MESSAGE_TOPIC, BOOTSTRAP_SERVERS);
//            ArchiveHandler archiveHandler = new ArchiveHandler(ARCHIVE_TOPIC, 10000);


            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down...");
                running.set(false);
                kafkaConsumer.close();
            }));
            while (running.get()) {
                List<String> messages = kafkaConsumer.consumeMessages();
                for (String message : messages) {
                    Set<ValidationMessage> errors = validator.validateAndProcessMessage(message);

                    if (errors != null && !errors.isEmpty()) {
                        System.out.println("Invalid message: " + message);
//                        invalidMessageChannel.publishInvalidMessage(message, errors);
                    }
                    WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
                    System.out.println(weatherMessage);
//                    archiveHandler.receiveWeatherMessage(weatherMessage);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
