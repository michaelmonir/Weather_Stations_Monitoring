package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

@SpringBootApplication
public class ConsumerApplication {

	public static KafkaConsumer getConsumer() {
		Properties kafkaProps = new Properties();
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-service:9092");
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "g3");
		kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer consumer = new KafkaConsumer<>(kafkaProps);

		consumer.subscribe(Collections.singletonList("mic1"));
		return consumer;
	}

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);

		KafkaConsumer<String, String> consumer = getConsumer();

		while (true) {
//			long start = System.currentTimeMillis();
			consumer.poll(Duration.ofMillis(500))
					.forEach(ConsumerApplication::printRecord);
//			long end = System.currentTimeMillis();
//			System.out.println("Time taken: " + (end - start));
		}
	}

	public static void printRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
		System.out.println("sent on the topic " + record.topic() + " and message is " + record.value());
	}
}
