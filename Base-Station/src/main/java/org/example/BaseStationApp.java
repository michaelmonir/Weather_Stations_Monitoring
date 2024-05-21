package org.example;

import com.networknt.schema.ValidationMessage;
import org.example.apis.InvalidMessageChannel;
import org.example.apis.KafkaMessageConsumer;
import org.example.bitcasktranslator.MessageTranslator;
import org.example.models.WeatherMessage;
import org.example.parquet.writers.Archiver;
import org.example.parquet.writers.SparkArchiveHandler;
import org.example.validators.MessageValidator;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseStationApp {

    private static final String SCHEMA_FILE_PATH = "/home/ahmed/Desktop/Weather_Stations_Monitoring/Base-Station/src/main/resources/weather-station-schema.json";

    private static final String TOPIC_NAME = "weather-station";
    private static final int BATCH_SIZE = 1000;

    private static final String INVALID_MESSAGE_TOPIC = "invalid-messages";
    private static final String ARCHIVE_TOPIC = "parquet-archive";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String BitCask_IP = "localhost";
    private static final int BitCask_PORT = 12345;

    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        try {
            InputStream schemaStream = Files.newInputStream(Paths.get(SCHEMA_FILE_PATH));
            KafkaMessageConsumer kafkaConsumer = new KafkaMessageConsumer(TOPIC_NAME, BOOTSTRAP_SERVERS);
            MessageValidator validator = new MessageValidator(schemaStream);
            InvalidMessageChannel invalidMessageChannel = new InvalidMessageChannel(INVALID_MESSAGE_TOPIC, BOOTSTRAP_SERVERS);
            Archiver archiveHandler = new SparkArchiveHandler(ARCHIVE_TOPIC, BATCH_SIZE);
            MessageTranslator messageTranslator = new MessageTranslator();

            Socket socket = connectToServer(BitCask_IP, BitCask_PORT);

            Socket finalSocket = socket;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down...");
                running.set(false);
                kafkaConsumer.close();
                try {
                    if (finalSocket != null) {
                        finalSocket.close();
                    }
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
                        System.out.println("Invalid message: " + message);
                        invalidMessageChannel.publishInvalidMessage(message, errors);
                        continue;
                    }
                    WeatherMessage weatherMessage = WeatherMessage.fromJson(message);
                    System.out.println("Received message: " + weatherMessage.toString());
                    archiveHandler.receiveWeatherMessage(weatherMessage);
                    assert weatherMessage != null;
                    if (socket == null || socket.isClosed() || !socket.isConnected()) {
                        socket = connectToServer(BitCask_IP, BitCask_PORT);
                    }
                    socket.getOutputStream().write(messageTranslator.translate(weatherMessage));
                }
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static Socket connectToServer(String host, int port) {
        while (true) {
            try {
                return new Socket(host, port);
            } catch (IOException e) {
                System.out.println("Failed to connect to server, retrying in 5 seconds...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
