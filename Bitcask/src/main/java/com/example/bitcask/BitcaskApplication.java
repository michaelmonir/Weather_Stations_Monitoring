package com.example.bitcask;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Compaction.CompactionScheduler;
import com.example.bitcask.Compaction.CompactionStrategyImpl;
import com.example.bitcask.Compaction.Compactor;
import com.example.bitcask.Compaction.IntervalMerging.IntervalMergerStrategyDevideAndConquerImpl;
import com.example.bitcask.Message.Message;
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

//		Compactor compactor = new Compactor(
//				new CompactionStrategyImpl(),
//				new IntervalMergerStrategyDevideAndConquerImpl());
//		compactor.compact();

		Bitcask bitcask = Bitcask.getBitcask();

		int z = 0;
		z++;

		for (int i = 1; i <= 20; i++) {
			Message message = Bitcask.getBitcask().read((long)i);
			System.out.println(message.toString());
		}
	}
}
