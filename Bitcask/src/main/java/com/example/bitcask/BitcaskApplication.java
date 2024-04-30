package com.example.bitcask;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.File.TextFileAtmoicUpdator;
import com.example.bitcask.Message.Message;
import com.example.bitcask.Recovery.RecoveryInformationKeeper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class BitcaskApplication {

	public static void main(String[] args) {
		SpringApplication.run(BitcaskApplication.class, args);

		String fileName = "recoveryInformation.txt";
		List<Integer> list = new ArrayList<>();
		list.add(1); list.add(2);
		TextFileAtmoicUpdator.writeList(list, fileName);

//		RecoveryInformationKeeper recoveryInformationKeeper = new RecoveryInformationKeeper();
//		Bitcask.setBitcask(recoveryInformationKeeper.recover());
//
//		for (long i = 1; i <= 21; i++) {
//			Message message = new Message(i, i, (short)i, i, (int)i, (int)i, (int)i);
//			Bitcask.getBitcask().write(message);
//		}
//		for (int i = 1; i <= 20; i++) {
//			Message message = Bitcask.getBitcask().read((long)i);
//			System.out.println(message.toString());
//		}
	}
}
