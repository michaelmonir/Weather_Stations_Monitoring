package com.example;

public class Main {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String inputTopic = "weather-station";
        String outputTopic = "rain-alerts";

        // Create an instance of KafkaStreamsAPI
        RunningDetector kafkaStreamsAPI = new RunningDetector(bootstrapServers, inputTopic, outputTopic);

        try {
            kafkaStreamsAPI.start();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaStreamsAPI.stop();
        }
    }
}
