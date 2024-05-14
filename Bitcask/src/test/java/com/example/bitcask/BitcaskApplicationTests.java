package com.example.bitcask;

import com.example.bitcask.Bitcask.BitcaskRunner;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewMerge.Merger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BitcaskApplicationTests {

	@Test
	public void testContinuousMerge() {
		BitcaskRunner.start(10, 15L);
		int iterations = 1000;

		Thread thread = new Thread(new MergeScheduler());
		thread.start();

		for (int i = 0; i < 1000; i++) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
			BitcaskRunner.put((long)i);
		}
	}

	@Test
	public void testContinuousMerge2() {
		BitcaskRunner.start(10, 15L);

		for (int i = 0; i < 10_000; i++) {
			BitcaskRunner.put(i);
			Merger merger = new Merger();
			merger.run();
		}
		for (int i = 0; i < 10_000; i++) {
			Message message = BitcaskRunner.read((long)i);
			Assertions.assertEquals(i, message.getS_no());
		}
	}

	@Test
	public void withoutMergeAndLargeFiles() {
		BitcaskRunner.start(1_000_000, 15L);

		for (int i = 0; i < 100_000; i++) {
			BitcaskRunner.put(i);
		}
		for (int i = 0; i < 100_000; i++) {
			Message message = BitcaskRunner.read(i);
			Assertions.assertEquals(i, message.getS_no());
		}
	}
}

