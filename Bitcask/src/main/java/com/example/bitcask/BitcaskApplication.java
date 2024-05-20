package com.example.bitcask;

import com.example.bitcask.Converters.Message.ByteToMessageConverter;
import com.example.bitcask.Converters.Message.MessageToByteConverter;
import com.example.bitcask.Message.Message;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BitcaskApplication {

	public static void main(String[] args) {
		SpringApplication.run(BitcaskApplication.class, args);

		Long timestamp = System.currentTimeMillis();
		System.out.println(timestamp);

		Message message = new Message(1L, 1L, (short)1, timestamp, 1, 1, 1);
		byte[] bytes = new MessageToByteConverter(message).convert();
		Message message2 = new ByteToMessageConverter(bytes).convert();

		System.out.println(message2.getStatus_timestamp());
	}
}
