package com.example.bitcask;

import com.example.bitcask.Bitcask.BitcaskRunner;
import com.example.bitcask.Converters.Message.ByteToMessageConverter;
import com.example.bitcask.Converters.Message.MessageToByteConverter;
import com.example.bitcask.Message.Message;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

@SpringBootApplication
public class BitcaskApplication {

	private final static int MAX_NUM_OF_SEGMENTS = 5;
	private final static int MAX_SEGMENT_SIZE = 10000000;
	private final static long MERGE_SCHEDULE_TIME = 1000;

	private final static int PORT = 12345;

	public static void main(String[] args) {
		SpringApplication.run(BitcaskApplication.class, args);
		BitcaskRunner.start(MAX_SEGMENT_SIZE, MAX_NUM_OF_SEGMENTS, MERGE_SCHEDULE_TIME);

		try (ServerSocket serverSocket = new ServerSocket(PORT)) {
			System.out.println("Server started. Waiting for clients...");

			while (true) {
				Socket clientSocket = serverSocket.accept();
				Thread clientHandlerThread = new Thread(() -> handleClient(clientSocket));
				clientHandlerThread.start();
			}

		} catch (IOException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private static void handleClient(Socket clientSocket) {
		try {
			InputStream inputStream = clientSocket.getInputStream();
			byte[] buffer = new byte[Message.MESSAGE_SIZE];
			while ((inputStream.read(buffer)) != -1) {
				Message receivedMessage = new ByteToMessageConverter(buffer).convert();
				System.out.println("Received message: " + receivedMessage.toString());
				BitcaskRunner.put(receivedMessage);
				System.out.println(BitcaskRunner.read(receivedMessage.getStation_id()));
			}
			clientSocket.close();
		} catch (IOException e) {
			System.out.println("Error handling client: " + e.getMessage());
		}
	}
}
