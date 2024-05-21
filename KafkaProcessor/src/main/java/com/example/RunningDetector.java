package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class RunningDetector {

    private final Properties properties;
    private final String inputTopic;
    private final String outputTopic;

    public RunningDetector(String bootstrapServers, String inputTopic, String outputTopic) {
        this.properties = new Properties();
        this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector");
        this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass().getName());
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public void start() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        KStream<String, String> weatherStream = builder.stream(inputTopic);
        weatherStream.filter((key, value) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode jsonNode = mapper.readTree(value);
                        int humidity = jsonNode.path("weather").path("humidity").asInt();
                        return humidity > 70;
                    } catch (Exception e) {
                        System.out.println("Error parsing JSON: " + e.getMessage());
                        return false;
                    }
                })
                .mapValues(value -> {
                            int stationId = 0;
                            int sNo = 0;
                            String batteryStatus = "";
                            long timestamp = 0;
                            int humidity = 0;

                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                JsonNode jsonNode = mapper.readTree(value);
                                stationId = jsonNode.path("station_id").asInt();
                                sNo = jsonNode.path("s_no").asInt();
                                batteryStatus = jsonNode.path("battery_status").asText();
                                timestamp = jsonNode.path("status_timestamp").asLong();
                                humidity = jsonNode.path("weather").path("humidity").asInt();
                            } catch (JsonProcessingException e) {
                                return null;
                            }

                            String format = String.format("{\n" +
                                    "    \"message\": \"Rain alert\",\n" +
                                    "    \"station_id\": %d,\n" +
                                    "    \"s_no\": %d,\n" +
                                    "    \"battery_status\": \"%s\",\n" +
                                    "    \"timestamp\": %d,\n" +
                                    "    \"humidity\": %s\n" +
                                    "}", stationId, sNo, batteryStatus, timestamp, humidity);
                            return format;
                        }
                )
                .peek((key, value) -> System.out.println("Rain alert: " + value))
                .filter((key, value) -> value != null)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }


    public void stop() {

    }

}