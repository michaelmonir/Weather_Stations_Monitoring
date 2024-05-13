package com.example.bitcask;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.BitcaskRunner;
import com.example.bitcask.NewMerge.MergeScheduler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BitcaskApplicationTests {

	@Test
	public void testContinuousMerge() {
		BitcaskRunner.start(10, 1000L);

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
}
