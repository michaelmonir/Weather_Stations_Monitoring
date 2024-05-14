package org.example.basestation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class WeatherStationConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WeatherStationConsumer.class);
    private static final String TOPIC_NAME = "weather-station";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_FILE_PATH = "weather-station-schema.json";

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final JsonSchema schema;

    public WeatherStationConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-station-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        this.objectMapper = new ObjectMapper();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        try (InputStream schemaStream = WeatherStationConsumer.class.getClassLoader().getResourceAsStream(SCHEMA_FILE_PATH)) {
            this.schema = factory.getSchema(schemaStream);
        } catch (Exception e) {
            throw new RuntimeException("Error loading JSON schema", e);
        }
    }

    public void consume() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received message: {}", record.value());

                try {
                    JsonNode jsonNode = objectMapper.readTree(record.value());
                    Set<ValidationMessage> errors = schema.validate(jsonNode);
                    if (!errors.isEmpty()) {
                        logger.error("Invalid JSON message: {}", errors);
                        publishToDeadLetterTopic(record.value());
                    }
                    processValidMessage(jsonNode);

                } catch (Exception e) {
                    logger.error("Error validating or processing JSON message: {}", e.getMessage());
                }
            }
        }
    }

    private void publishToDeadLetterTopic(String value) {
        logger.info("Publishing to dead-letter topic: {}", value);
        //TODO: Implement the logic to publish the message to a dead-letter topic
    }

    private void processValidMessage(JsonNode jsonNode) {
        long stationId = jsonNode.get("station_id").asLong();
        long sequenceNumber = jsonNode.get("s_no").asLong();
        String batteryStatus = jsonNode.get("battery_status").asText();
        long statusTimestamp = jsonNode.get("status_timestamp").asLong();

        JsonNode weatherNode = jsonNode.get("weather");
        int humidity = weatherNode.get("humidity").asInt();
        int temperature = weatherNode.get("temperature").asInt();
        int windSpeed = weatherNode.get("wind_speed").asInt();

        // Process the extracted data as needed
        logger.info("Station ID: {}", stationId);
        logger.info("Sequence Number: {}", sequenceNumber);
        logger.info("Battery Status: {}", batteryStatus);
        logger.info("Status Timestamp: {}", statusTimestamp);
        logger.info("Humidity: {}", humidity);
        logger.info("Temperature: {}", temperature);
        logger.info("Wind Speed: {}", windSpeed);
        logger.info("--------------------");
    }

    public void close() {
        consumer.close();
    }
}
