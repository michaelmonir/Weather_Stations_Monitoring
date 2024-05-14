package com.example.bitcask;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.Merger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BitcaskApplication {

	public static void main(String[] args) {
		SpringApplication.run(BitcaskApplication.class, args);

//		Thread thread = new Thread(new CompactionScheduler());
//		thread.start();

//		RecoveryInformationKeeper recoveryInformationKeeper = new RecoveryInformationKeeper();
//		Bitcask.setBitcask(recoveryInformationKeeper.recover());

		for (long i = 1; i <= 20; i++) {
			Message message = new Message(i, i, (short)i, i, (int)i, (int)i, (int)i);
			Bitcask.getBitcask().write(message);
		}

		Merger merger = new Merger();
		merger.run();

		Bitcask bitcask = Bitcask.getBitcask();

		for (int i = 1; i <= 20; i++) {
			Message message = Bitcask.getBitcask().read((long)i);
			System.out.println(message.toString());
		}
	}


}
