package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class ProducerApplication {

	public static KafkaProducer getProducer() {
		Properties kafkaProps = new Properties();
//		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-service:9092");
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new KafkaProducer<>(kafkaProps);
	}

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);

		String topic = "mic1", message = "mic";
		KafkaProducer<String, String> producer = getProducer();

		while (true) {
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
//			long start = System.currentTimeMillis();
			try {
				System.out.println(producer.send(record).get().toString());
				System.out.printf("sent on the topic %s and message is %s%n", record.topic(), record.value());
			} catch (Exception e) {
				e.printStackTrace();
			}
//			long end = System.currentTimeMillis();
//			System.out.println("Time taken: " + (end - start));
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
//		producer.close();
	}
}
