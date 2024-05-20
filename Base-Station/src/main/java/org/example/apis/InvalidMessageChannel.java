package org.example.apis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.ValidationMessage;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.models.InvalidMessage;

import java.util.*;

public class InvalidMessageChannel {
    private final KafkaProducer<String, String> producer;
    private final String errorTopic;
    private final String bootstrapServers;
    private final ObjectMapper objectMapper;

    public InvalidMessageChannel(String errorTopic, String bootstrapServers) {
        this.errorTopic = errorTopic;
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = new ObjectMapper();
        this.producer = createKafkaProducer();

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void publishInvalidMessage(String message, Set<ValidationMessage> errors) {
        try {
            InvalidMessage invalidMessage = new InvalidMessage(message, errors);
            String json = objectMapper.writeValueAsString(invalidMessage);

            ProducerRecord<String, String> record = new ProducerRecord<>(errorTopic, json);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void close() {
        producer.close();
    }
}
